using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Util;
using Raven.Server.Documents.CdcSink.Commands;
using Raven.Server.Documents.CdcSink.Stats;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// CDC Sink process that pulls change data from PostgreSQL using logical replication streaming.
///
/// <para><b>Startup:</b> Creates a publication (<c>CREATE PUBLICATION</c>) covering all configured tables
/// and a logical replication slot (<c>pg_create_logical_replication_slot</c>) using the <c>pgoutput</c>
/// plugin, if they don't already exist. If the connection lacks permissions, the error includes the exact
/// admin SQL to run. Existing publications/slots (e.g., pre-created by an admin) are reused.</para>
///
/// <para><b>Initial Load:</b> Before streaming, performs a full table scan of each configured table using
/// keyset pagination (ordered by primary key). Rows are processed through <see cref="CdcSinkDocumentProcessor"/>
/// and written to RavenDB. Progress (last PK values per table) is persisted so a restart resumes from
/// where it left off. Batch submission is pipelined: the next batch is read while the previous one is
/// being written by the transaction merger.</para>
///
/// <para><b>Replication Streaming:</b> After the initial load, opens a <see cref="LogicalReplicationConnection"/>
/// and starts streaming from the replication slot at the last acknowledged LSN. Messages arrive as
/// <c>InsertMessage</c>, <c>UpdateMessage</c>, <c>KeyDeleteMessage</c>, or <c>FullDeleteMessage</c> inside
/// <c>BeginMessage</c>/<c>CommitMessage</c> transaction boundaries. Rows within a transaction are buffered
/// in a pending list and moved to the batch on commit, so partial transactions are never written to RavenDB.
/// Batch submission is pipelined with reading: while the transaction merger processes one batch, the next
/// messages are read from the replication stream.</para>
///
/// <para><b>Consistency guarantee:</b> PostgreSQL logical replication is push-based — the server streams
/// changes in commit order and the replication slot tracks consumer progress. After each batch is written,
/// the acknowledged LSN is reported back via <c>SetReplicationStatus</c> + <c>SendStatusUpdate</c>.
/// This ensures PostgreSQL retains WAL segments until we confirm receipt, so no changes are lost as long
/// as the slot exists. Column types are resolved from the <c>RelationMessage</c> OIDs that PostgreSQL
/// sends inline in the replication stream.</para>
/// </summary>
public class PostgresCdcSinkProcess : CdcSinkProcess
{
    private readonly CdcSinkDocumentProcessor _documentProcessor;
    private readonly string _connectionString;
    private string _publicationName;
    private string _slotName;

    // Cache resolved type categories per RelationMessage OID layout.
    // Column types are fixed per relation in the replication stream.
    private readonly Dictionary<string, PostgresTypeCategory[]> _relationTypeCache = new();

    public PostgresCdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
        : base(configuration, database)
    {
        _documentProcessor = new CdcSinkDocumentProcessor(configuration);
        _connectionString = configuration.Connection.ConnectionString;
    }

    protected override void Run()
    {
        AsyncHelpers.RunSync(() => RunAsync(CancellationToken));
    }

    private async Task RunAsync(CancellationToken ct)
    {
        try
        {
            await EnsureReplicationSetup(ct);
            await HandleInitialLoad(ct);
            await StartListening(ct);
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown
        }
        catch (Exception e)
        {
            if (Logger.IsErrorEnabled)
                Logger.Error($"[{Name}] CDC Sink process failed.", e);

            var alert = AlertRaised.Create(
                Database.Name, Tag,
                $"[{Name}] CDC Sink process failed: {e.Message}",
                AlertReason.CdcSink_Error,
                NotificationSeverity.Error,
                key: $"{Tag}/{Name}",
                details: new ExceptionDetails(e));

            Database.NotificationCenter.Add(alert);
            EnterFallbackMode();
        }
    }

    private async Task EnsureReplicationSetup(CancellationToken ct)
    {
        var tableNames = Configuration.CollectAllSourceTableNames("public");
        _publicationName = CdcSinkSourceVerifier.ComputePublicationName(tableNames);
        _slotName = CdcSinkSourceVerifier.ComputeSlotName(tableNames);

        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        await using (var cmd = new NpgsqlCommand("SELECT 1 FROM pg_publication WHERE pubname = @pubName", conn))
        {
            cmd.Parameters.AddWithValue("pubName", _publicationName);
            var exists = await cmd.ExecuteScalarAsync(ct);

            if (exists == null)
            {
                var tableList = string.Join(", ", tableNames.Select(t =>
                {
                    var parts = t.Split('.');
                    return parts.Length == 2 ? $"{parts[0]}.{parts[1]}" : t;
                }));

                try
                {
                    await using var createCmd = new NpgsqlCommand(
                        $"CREATE PUBLICATION {_publicationName} FOR TABLE {string.Join(", ", tableList)}", conn);
                    await createCmd.ExecuteNonQueryAsync(ct);
                }
                catch (PostgresException ex) when (ex.SqlState == "42501")
                {
                    throw new InvalidOperationException(
                        $"""
                        Insufficient permissions to create publication '{_publicationName}'. The database user must have CREATE permission on the database, or an administrator can create the publication manually:

                          CREATE PUBLICATION {_publicationName} FOR TABLE {tableList};

                        PostgreSQL error: {ex.MessageText}
                        """, ex);
                }
            }
        }

        // Check if replication slot already exists before trying to create it.
        // This avoids permission errors when the slot was created by an admin but
        // the current user lacks the REPLICATION role attribute.
        bool slotExists;
        await using (var cmd = new NpgsqlCommand("SELECT 1 FROM pg_replication_slots WHERE slot_name = @slotName", conn))
        {
            cmd.Parameters.AddWithValue("slotName", _slotName);
            slotExists = await cmd.ExecuteScalarAsync(ct) != null;
        }

        if (slotExists == false)
        {
            try
            {
                await using var cmd = new NpgsqlCommand(
                    $"SELECT pg_create_logical_replication_slot('{_slotName}', 'pgoutput')", conn);
                await cmd.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException ex) when (ex.SqlState == "42710")
            {
                // Race condition: slot was created between our check and create — safe to ignore
            }
            catch (PostgresException ex) when (ex.SqlState == "42501")
            {
                throw new InvalidOperationException(
                    $"""
                    Insufficient permissions to create replication slot '{_slotName}'. The database user must have the REPLICATION role attribute, or an administrator can create the slot manually:

                      SELECT pg_create_logical_replication_slot('{_slotName}', 'pgoutput');

                    PostgreSQL error: {ex.MessageText}
                    """, ex);
            }
        }
    }

    private async Task StartListening(CancellationToken ct)
    {
        NpgsqlTypes.NpgsqlLogSequenceNumber lastLsn;
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var state = LoadState(context);
            lastLsn = string.IsNullOrEmpty(state.LastLsn)
                ? new NpgsqlTypes.NpgsqlLogSequenceNumber(0)
                : new NpgsqlTypes.NpgsqlLogSequenceNumber(ulong.Parse(state.LastLsn));
        }

        await using var conn = new LogicalReplicationConnection(_connectionString);
        await conn.Open(ct);

        var replicationStream = conn.StartReplication(
            new PgOutputReplicationSlot(_slotName),
            new PgOutputReplicationOptions(_publicationName, PgOutputProtocolVersion.V1),
            ct,
            lastLsn);

        var batch = new List<CdcSinkDocumentOp>();
        var pending = new List<CdcSinkDocumentOp>();
        Task lastBatch = Task.CompletedTask;
        int rowsSinceLastAck = 0;
        int maxBatchSize = Database.Configuration.CdcSink.MaxBatchSize;

        await using var enumerator = replicationStream.GetAsyncEnumerator(ct);
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var moveNext = enumerator.MoveNextAsync();

            if (moveNext.IsCompleted == false)
            {
                var moveTask = moveNext.AsTask();

                // Race: wait for either the next message or the previous batch to complete.
                // This allows reading ahead while the TxMerger processes the previous batch.
                await Task.WhenAny(moveTask, lastBatch);

                if (lastBatch.IsCompleted)
                {
                    await lastBatch;

                    // No more records available right now — acknowledge everything we've persisted.
                    // Only send status updates if we got any records since the last time
                    // Important: we must also only send it the _first_ time after we recieve _a_ value
                    if (rowsSinceLastAck is not 0)
                    {
                        conn.SetReplicationStatus(lastLsn);
                        await conn.SendStatusUpdate(ct);
                        rowsSinceLastAck = 0;
                    }

                    if (batch.Count > 0)
                    {
                        lastBatch = SubmitBatch(batch, lastLsn.ToString());
                        batch = new List<CdcSinkDocumentOp>();
                    }
                }

                await moveTask;
            }

            var message = enumerator.Current;

            switch (message)
            {
                case InsertMessage insert:
                    pending.Add(await DecodeRow(insert.Relation, insert.NewRow, CdcSinkOperation.Upsert));
                    break;
                case UpdateMessage update:
                    pending.Add(await DecodeRow(update.Relation, update.NewRow, CdcSinkOperation.Upsert));
                    break;
                case KeyDeleteMessage keyDel:
                    pending.Add(await DecodeRow(keyDel.Relation, keyDel.Key, CdcSinkOperation.Delete));
                    break;
                case FullDeleteMessage fullDel:
                    pending.Add(await DecodeRow(fullDel.Relation, fullDel.OldRow, CdcSinkOperation.Delete));
                    break;
                case BeginMessage:
                    break;
                case CommitMessage commit:
                    batch.AddRange(pending);
                    pending.Clear();

                    if (lastBatch.IsCompleted || ShouldFlushBatch(batch.Count))
                    {
                        await lastBatch;

                        if (batch.Count > 0)
                        {
                            rowsSinceLastAck += batch.Count;
                            lastBatch = SubmitBatch(batch, commit.CommitLsn.ToString());
                            lastLsn = commit.CommitLsn;
                            batch = new List<CdcSinkDocumentOp>();
                        }

                        // Acknowledge to PostgreSQL periodically — when we've persisted enough rows,
                        // rather than on every batch flush. We'll either consume a enough records to
                        // flush, or go idle and send the update higher in this method
                        if (rowsSinceLastAck >= maxBatchSize)
                        {
                            conn.SetReplicationStatus(lastLsn);
                            await conn.SendStatusUpdate(ct);
                            rowsSinceLastAck = 0;
                        }
                    }
                    break;
            }
        }
    }

    private async Task<CdcSinkDocumentOp> DecodeRow(
        RelationMessage relation, ReplicationTuple row, CdcSinkOperation operation)
    {
        var relationKey = $"{relation.Namespace}.{relation.RelationName}";

        if (_relationTypeCache.TryGetValue(relationKey, out var typeCategories) == false)
        {
            typeCategories = BuildTypeCategoriesFromRelation(relation);
            _relationTypeCache[relationKey] = typeCategories;
        }

        var data = new Dictionary<string, object>();
        int columnIndex = 0;

        await foreach (var item in row)
        {
            var columnName = item.GetFieldName();
            var category = columnIndex < typeCategories.Length ? typeCategories[columnIndex] : PostgresTypeCategory.Other;
            var value = item.IsDBNull ? null : await item.Get();
            data[columnName] = ConvertPostgresValue(category, value);

            columnIndex++;
        }

        var cdcRow = new CdcSinkRow
        {
            TableSchema = relation.Namespace,
            TableName = relation.RelationName,
            Operation = operation,
            Data = data,
        };

        return _documentProcessor.ProcessRow(cdcRow);
    }

    /// <summary>
    /// Build type category array from the RelationMessage's column OIDs.
    /// PostgreSQL OIDs are well-known and documented.
    /// </summary>
    private static PostgresTypeCategory[] BuildTypeCategoriesFromRelation(RelationMessage relation)
    {
        var categories = new PostgresTypeCategory[relation.Columns.Count];
        for (int i = 0; i < relation.Columns.Count; i++)
        {
            categories[i] = OidToCategory(relation.Columns[i].DataTypeId);
        }
        return categories;
    }

    private static PostgresTypeCategory OidToCategory(uint oid)
    {
        return oid switch
        {
            21 or 23 or 26 => PostgresTypeCategory.Integer,    // int2, int4, oid
            20 => PostgresTypeCategory.BigInt,                  // int8
            700 => PostgresTypeCategory.Float,                  // float4
            701 => PostgresTypeCategory.Double,                 // float8
            1700 => PostgresTypeCategory.Numeric,               // numeric/decimal
            16 => PostgresTypeCategory.Boolean,                 // bool
            1082 or 1114 or 1184 => PostgresTypeCategory.DateTime, // date, timestamp, timestamptz
            2950 => PostgresTypeCategory.Uuid,                  // uuid
            17 => PostgresTypeCategory.Bytea,                   // bytea
            114 or 3802 => PostgresTypeCategory.Json,           // json, jsonb
            _ => PostgresTypeCategory.Other,
        };
    }

    private enum PostgresTypeCategory
    {
        Other,
        Integer,
        BigInt,
        Float,
        Double,
        Numeric,
        Boolean,
        DateTime,
        Uuid,
        Bytea,
        Json
    }

    private Task SubmitBatch(List<CdcSinkDocumentOp> ops, string lastLsn = null,
        Dictionary<string, CdcSinkTableLoadState> tableLoadUpdates = null)
    {
        var command = new CdcSinkBatchCommand(
            Database, ops, Configuration.Name, lastLsn,
            tableLoadUpdates: tableLoadUpdates,
            patchRequest: _documentProcessor.CombinedPatchRequest,
            statsScope: null, statistics: Statistics, logger: Logger);

        return Database.TxMerger.Enqueue(command);
    }

    private async Task HandleInitialLoad(CancellationToken ct)
    {
        CdcSinkTaskState state;
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            state = LoadState(context);
        }

        var allTables = Configuration.CollectAllTablesFlat("public");

        foreach (var tableInfo in allTables)
        {
            var tableKey = CdcSinkSourceVerifier.ComputeTablesHash(new List<string> { tableInfo.FullName });

            if (state.Tables.TryGetValue(tableKey, out var tableState) && tableState.InitialLoadCompleted)
                continue;

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] Starting initial load for table {tableInfo.FullName}");

            await ProcessTableInitialLoad(tableInfo, tableKey, tableState, ct);

            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] Completed initial load for table {tableInfo.FullName}");
        }
    }

    private async Task ProcessTableInitialLoad(
        CdcSinkConfiguration.TableInfo tableInfo, string tableKey, CdcSinkTableLoadState resumeState, CancellationToken ct)
    {
        var pkColumns = tableInfo.PrimaryKeyColumns;
        var maxBatchSize = Database.Configuration.CdcSink.MaxBatchSize;

        string[] lastKeys = null;
        if (resumeState?.LastKeyValues != null && resumeState.LastKeyValues.Count == pkColumns.Count)
        {
            lastKeys = new string[resumeState.LastKeyValues.Count];
            for (int i = 0; i < resumeState.LastKeyValues.Count; i++)
                lastKeys[i] = resumeState.LastKeyValues[i];
        }

        // Single connection for the entire initial load of this table.
        // Read one batch at a time with LIMIT; while the previous batch
        // is being applied by the TxMerger, we read the next batch.
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        // Fetch column types once for the entire initial load — used for keyset pagination parameter types
        var columnTypes = await GetColumnTypes(conn, tableInfo.Schema, tableInfo.TableName, pkColumns, ct);

        var lastBatch = Task.CompletedTask;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var (ops, newLastKeys) = await ReadOneBatch(conn, tableInfo, pkColumns, lastKeys, maxBatchSize, columnTypes, ct);

            if (ops.Count == 0)
            {
                await lastBatch;

                var finalUpdate = new Dictionary<string, CdcSinkTableLoadState>
                {
                    [tableKey] = new CdcSinkTableLoadState { InitialLoadCompleted = true }
                };
                await SubmitBatch([], tableLoadUpdates: finalUpdate);
                break;
            }

            await lastBatch;

            var tableLoadUpdate = new Dictionary<string, CdcSinkTableLoadState>
            {
                [tableKey] = new CdcSinkTableLoadState { LastKeyValues = [.. newLastKeys] }
            };

            lastBatch = SubmitBatch(ops, tableLoadUpdates: tableLoadUpdate);
            lastKeys = newLastKeys;
        }

        await lastBatch;
    }

    private async Task<(List<CdcSinkDocumentOp> Ops, string[] LastKeys)> ReadOneBatch(
        NpgsqlConnection conn, CdcSinkConfiguration.TableInfo tableInfo,
        List<string> pkColumns, string[] lastKeys, int maxBatchSize,
        Dictionary<string, string> columnTypes, CancellationToken ct)
    {
        var orderBy = string.Join(", ", pkColumns);

        NpgsqlCommand cmd;
        if (lastKeys != null)
        {
            var paramPlaceholders = new string[pkColumns.Count];
            for (int i = 0; i < pkColumns.Count; i++)
                paramPlaceholders[i] = $"@k{i}";

            var query = $"SELECT * FROM {tableInfo.FullName} WHERE ({string.Join(", ", pkColumns)}) > ({string.Join(", ", paramPlaceholders)}) ORDER BY {orderBy} LIMIT {maxBatchSize}";
            cmd = new NpgsqlCommand(query, conn);

            for (int i = 0; i < pkColumns.Count; i++)
            {
                var value = ConvertStringToType(lastKeys[i], columnTypes.GetValueOrDefault(pkColumns[i], "text"));
                cmd.Parameters.AddWithValue($"k{i}", value);
            }
        }
        else
        {
            var query = $"SELECT * FROM {tableInfo.FullName} ORDER BY {orderBy} LIMIT {maxBatchSize}";
            cmd = new NpgsqlCommand(query, conn);
        }

        await using (cmd)
        {
            await using var reader = await cmd.ExecuteReaderAsync(ct);

            var ops = new List<CdcSinkDocumentOp>();

            while (await reader.ReadAsync(ct))
            {
                var data = new Dictionary<string, object>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var name = reader.GetName(i);
                    var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    data[name] = value;
                }

                var row = new CdcSinkRow
                {
                    TableSchema = tableInfo.Schema,
                    TableName = tableInfo.TableName,
                    Operation = CdcSinkOperation.Upsert,
                    Data = data,
                };

                var op = _documentProcessor.ProcessRow(row);
                ops.Add(op);
            }

            // Extract last keys from the last row's RawData for keyset pagination resume
            string[] newLastKeys = null;
            if (ops.Count > 0)
            {
                var lastRowData = ops[ops.Count - 1].RawData;
                newLastKeys = new string[pkColumns.Count];
                for (int i = 0; i < pkColumns.Count; i++)
                    newLastKeys[i] = lastRowData.TryGetValue(pkColumns[i], out var v) ? v?.ToString() ?? "" : "";
            }

            return (ops, newLastKeys);
        }
    }

    private static async Task<Dictionary<string, string>> GetColumnTypes(
        NpgsqlConnection conn, string schema, string tableName, List<string> columns, CancellationToken ct)
    {
        var types = new Dictionary<string, string>();
        var sql = @"SELECT column_name, data_type FROM information_schema.columns
                    WHERE table_schema = @schema AND table_name = @table AND column_name = ANY(@columns)";

        await using var cmd = new NpgsqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("schema", schema);
        cmd.Parameters.AddWithValue("table", tableName);
        cmd.Parameters.AddWithValue("columns", columns.ToArray());

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            types[reader.GetString(0)] = reader.GetString(1).ToLowerInvariant();

        return types;
    }

    private static object ConvertStringToType(string value, string normalizedType)
    {
        if (string.IsNullOrEmpty(value))
            return DBNull.Value;

        return normalizedType switch
        {
            "smallint" or "integer" or "serial" or "bigint" or "bigserial" => long.Parse(value),
            "real" or "double precision" or "numeric" or "decimal" => double.Parse(value),
            "boolean" => bool.Parse(value),
            "uuid" => Guid.Parse(value),
            _ => value,
        };
    }

    private static object ConvertPostgresValue(PostgresTypeCategory category, object value)
    {
        if (value is null || value == DBNull.Value)
            return null;

        return category switch
        {
            PostgresTypeCategory.Integer => Convert.ToInt64(value),
            PostgresTypeCategory.BigInt => Convert.ToInt64(value),
            PostgresTypeCategory.Float => Convert.ToDouble(value),
            PostgresTypeCategory.Double => Convert.ToDouble(value),
            PostgresTypeCategory.Numeric => Convert.ToDouble(value),
            PostgresTypeCategory.Boolean => Convert.ToBoolean(value),
            PostgresTypeCategory.DateTime => Convert.ToDateTime(value),
            PostgresTypeCategory.Uuid => value.ToString(),
            PostgresTypeCategory.Bytea => value,
            PostgresTypeCategory.Json => value.ToString(),
            _ => value,
        };
    }
}
