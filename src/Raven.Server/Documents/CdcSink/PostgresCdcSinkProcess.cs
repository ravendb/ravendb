using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
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

public class PostgresCdcSinkProcess : CdcSinkProcess
{
    private readonly CdcSinkDocumentProcessor _documentProcessor;
    private readonly string _connectionString;
    private string _publicationName;
    private string _slotName;

    public PostgresCdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
        : base(configuration, database)
    {
        _documentProcessor = new CdcSinkDocumentProcessor(configuration);
        _connectionString = configuration.Connection.ConnectionString;
    }

    protected override ICdcSinkConsumer CreateConsumer()
    {
        throw new NotSupportedException("PostgresCdcSinkProcess uses its own Run() loop instead of ICdcSinkConsumer.");
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
        var tableNames = CollectAllSourceTableNames();
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

                await using var createCmd = new NpgsqlCommand(
                    $"CREATE PUBLICATION {_publicationName} FOR TABLE {tableList}", conn);
                await createCmd.ExecuteNonQueryAsync(ct);
            }
        }

        try
        {
            await using var cmd = new NpgsqlCommand(
                $"SELECT pg_create_logical_replication_slot('{_slotName}', 'pgoutput')", conn);
            await cmd.ExecuteNonQueryAsync(ct);
        }
        catch (PostgresException ex) when (ex.SqlState == "42710")
        {
            // Replication slot already exists
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

        await using var enumerator = replicationStream.GetAsyncEnumerator();
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
                    conn.SetReplicationStatus(lastLsn);
                    await conn.SendStatusUpdate(ct);

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
                {
                    var ops = await DecodeRow(insert.Relation, insert.NewRow, CdcSinkOperation.Upsert);
                    pending.AddRange(ops);
                    break;
                }
                case UpdateMessage update:
                {
                    var ops = await DecodeRow(update.Relation, update.NewRow, CdcSinkOperation.Upsert);
                    pending.AddRange(ops);
                    break;
                }
                case KeyDeleteMessage keyDel:
                {
                    var ops = await DecodeRow(keyDel.Relation, keyDel.Key, CdcSinkOperation.Delete);
                    pending.AddRange(ops);
                    break;
                }
                case FullDeleteMessage fullDel:
                {
                    var ops = await DecodeRow(fullDel.Relation, fullDel.OldRow, CdcSinkOperation.Delete);
                    pending.AddRange(ops);
                    break;
                }
                case BeginMessage:
                    break;
                case CommitMessage commit:
                    batch.AddRange(pending);
                    pending.Clear();

                    if (lastBatch.IsCompleted)
                    {
                        await lastBatch;
                        conn.SetReplicationStatus(lastLsn);
                        await conn.SendStatusUpdate(ct);

                        if (batch.Count > 0)
                        {
                            lastBatch = SubmitBatch(batch, commit.CommitLsn.ToString());
                            lastLsn = commit.CommitLsn;
                            batch = new List<CdcSinkDocumentOp>();
                        }
                    }
                    else if (batch.Count >= Database.Configuration.CdcSink.MaxBatchSize)
                    {
                        await lastBatch;
                        conn.SetReplicationStatus(lastLsn);
                        await conn.SendStatusUpdate(ct);

                        lastBatch = SubmitBatch(batch, commit.CommitLsn.ToString());
                        lastLsn = commit.CommitLsn;
                        batch = new List<CdcSinkDocumentOp>();
                    }
                    break;
            }
        }
    }

    private async Task<List<CdcSinkDocumentOp>> DecodeRow(
        RelationMessage relation, ReplicationTuple row, CdcSinkOperation operation)
    {
        var data = new Dictionary<string, object>();
        await foreach (var item in row)
        {
            var columnName = item.GetFieldName();
            var value = item.IsDBNull ? null : await item.Get();
            data[columnName] = ConvertPostgresValue(item.GetDataTypeName(), value);
        }

        var cdcRow = new CdcSinkRow
        {
            TableSchema = relation.Namespace,
            TableName = relation.RelationName,
            Operation = operation,
            Data = data,
        };

        var op = _documentProcessor.ProcessRow(cdcRow);
        return new List<CdcSinkDocumentOp> { op };
    }

    private Task SubmitBatch(List<CdcSinkDocumentOp> ops, string lastLsn)
    {
        var command = new CdcSinkBatchCommand(
            Database, ops, Configuration.Name, lastLsn,
            tableLoadUpdates: null,
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

        var allTables = CollectAllTablesFlat();

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
        TableInfo tableInfo, string tableKey, CdcSinkTableLoadState resumeState, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        var pkColumns = tableInfo.PrimaryKeyColumns;
        var orderBy = string.Join(", ", pkColumns);
        var query = $"SELECT * FROM {tableInfo.FullName} ORDER BY {orderBy}";

        if (resumeState?.LastKeyValues != null && resumeState.LastKeyValues.Count == pkColumns.Count)
        {
            var columnTypes = await GetColumnTypes(conn, tableInfo.Schema, tableInfo.TableName, pkColumns, ct);
            var whereParts = $"({string.Join(", ", pkColumns)}) > ({string.Join(", ", pkColumns.Select((_, i) => $"@k{i}"))})";
            query = $"SELECT * FROM {tableInfo.FullName} WHERE {whereParts} ORDER BY {orderBy}";

            await using var cmd = new NpgsqlCommand(query, conn);
            for (int i = 0; i < pkColumns.Count; i++)
            {
                var value = ConvertStringToType(resumeState.LastKeyValues[i], columnTypes[pkColumns[i]]);
                cmd.Parameters.AddWithValue($"k{i}", value);
            }

            await ProcessInitialLoadReader(cmd, tableInfo, tableKey, pkColumns, ct);
        }
        else
        {
            await using var cmd = new NpgsqlCommand(query, conn);
            await ProcessInitialLoadReader(cmd, tableInfo, tableKey, pkColumns, ct);
        }
    }

    private async Task ProcessInitialLoadReader(
        NpgsqlCommand cmd, TableInfo tableInfo, string tableKey,
        List<string> pkColumns, CancellationToken ct)
    {
        await using var reader = await cmd.ExecuteReaderAsync(ct);

        var batch = new List<CdcSinkDocumentOp>();
        var maxBatchSize = Database.Configuration.CdcSink.MaxBatchSize ?? 1024;
        string[] lastKeyValues = null;

        while (await reader.ReadAsync(ct))
        {
            var data = new Dictionary<string, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                var name = reader.GetName(i);
                var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                data[name] = value;
            }

            lastKeyValues = pkColumns.Select(col => data.TryGetValue(col, out var v) ? v?.ToString() : "").ToArray();

            var row = new CdcSinkRow
            {
                TableSchema = tableInfo.Schema,
                TableName = tableInfo.TableName,
                Operation = CdcSinkOperation.Upsert,
                Data = data,
            };

            var op = _documentProcessor.ProcessRow(row);
            batch.Add(op);

            if (batch.Count >= maxBatchSize)
            {
                var loadUpdate = new Dictionary<string, CdcSinkTableLoadState>
                {
                    [tableKey] = new CdcSinkTableLoadState { LastKeyValues = lastKeyValues.ToList() }
                };

                var command = new CdcSinkBatchCommand(
                    Database, batch, Configuration.Name, lastLsn: null,
                    tableLoadUpdates: loadUpdate,
                    statsScope: null, statistics: Statistics, logger: Logger);

                Database.TxMerger.EnqueueSync(command);
                batch = new List<CdcSinkDocumentOp>();
            }
        }

        // Final batch + mark table complete
        var finalUpdate = new Dictionary<string, CdcSinkTableLoadState>
        {
            [tableKey] = new CdcSinkTableLoadState
            {
                InitialLoadCompleted = true,
                LastKeyValues = null,
            }
        };

        if (batch.Count > 0)
        {
            var command = new CdcSinkBatchCommand(
                Database, batch, Configuration.Name, lastLsn: null,
                tableLoadUpdates: finalUpdate,
                statsScope: null, statistics: Statistics, logger: Logger);

            Database.TxMerger.EnqueueSync(command);
        }
        else
        {
            // No rows remaining, but still need to mark complete
            var command = new CdcSinkBatchCommand(
                Database, new List<CdcSinkDocumentOp>(), Configuration.Name, lastLsn: null,
                tableLoadUpdates: finalUpdate,
                statsScope: null, statistics: Statistics, logger: Logger);

            Database.TxMerger.EnqueueSync(command);
        }
    }

    private CdcSinkTaskState LoadState(DocumentsOperationContext context)
    {
        var stateDocId = CdcSinkTaskState.GetDocumentId(Configuration.Name);
        var doc = Database.DocumentsStorage.Get(context, stateDocId);

        if (doc == null)
            return new CdcSinkTaskState { ConfigurationName = Configuration.Name };

        // Deserialize inline
        var data = doc.Data;
        var state = new CdcSinkTaskState();

        if (data.TryGet(nameof(CdcSinkTaskState.ConfigurationName), out string configName))
            state.ConfigurationName = configName;

        if (data.TryGet(nameof(CdcSinkTaskState.LastLsn), out string lastLsn))
            state.LastLsn = lastLsn;

        if (data.TryGet(nameof(CdcSinkTaskState.Tables), out BlittableJsonReaderObject tablesJson) && tablesJson != null)
        {
            var prop = new BlittableJsonReaderObject.PropertyDetails();
            for (int i = 0; i < tablesJson.Count; i++)
            {
                tablesJson.GetPropertyByIndex(i, ref prop);
                var key = prop.Name.ToString();

                if (prop.Value is BlittableJsonReaderObject tableJson)
                {
                    var ts = new CdcSinkTableLoadState();
                    if (tableJson.TryGet(nameof(CdcSinkTableLoadState.InitialLoadCompleted), out bool completed))
                        ts.InitialLoadCompleted = completed;
                    if (tableJson.TryGet(nameof(CdcSinkTableLoadState.LastKeyValues), out BlittableJsonReaderArray arr) && arr != null)
                    {
                        ts.LastKeyValues = new List<string>();
                        foreach (var k in arr)
                            ts.LastKeyValues.Add(k?.ToString());
                    }
                    state.Tables[key] = ts;
                }
            }
        }

        return state;
    }

    private List<string> CollectAllSourceTableNames()
    {
        var names = new List<string>();
        foreach (var table in Configuration.Tables)
        {
            var schema = table.SourceTableSchema ?? "public";
            names.Add($"{schema}.{table.SourceTableName}");
            CollectEmbeddedTableNames(table.EmbeddedTables, names);
        }
        return names;
    }

    private static void CollectEmbeddedTableNames(List<CdcSinkEmbeddedTableConfig> embedded, List<string> names)
    {
        if (embedded == null)
            return;

        foreach (var e in embedded)
        {
            var schema = e.SourceTableSchema ?? "public";
            names.Add($"{schema}.{e.SourceTableName}");
            CollectEmbeddedTableNames(e.EmbeddedTables, names);
        }
    }

    private List<TableInfo> CollectAllTablesFlat()
    {
        var tables = new List<TableInfo>();
        foreach (var table in Configuration.Tables)
        {
            tables.Add(new TableInfo
            {
                Schema = table.SourceTableSchema ?? "public",
                TableName = table.SourceTableName,
                PrimaryKeyColumns = table.PrimaryKeyColumns,
            });
            CollectEmbeddedTablesFlat(table.EmbeddedTables, tables);
        }
        return tables;
    }

    private static void CollectEmbeddedTablesFlat(List<CdcSinkEmbeddedTableConfig> embedded, List<TableInfo> tables)
    {
        if (embedded == null)
            return;

        foreach (var e in embedded)
        {
            tables.Add(new TableInfo
            {
                Schema = e.SourceTableSchema ?? "public",
                TableName = e.SourceTableName,
                PrimaryKeyColumns = e.PrimaryKeyColumns,
            });
            CollectEmbeddedTablesFlat(e.EmbeddedTables, tables);
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
            types[reader.GetString(0)] = reader.GetString(1);

        return types;
    }

    private static object ConvertStringToType(string value, string postgresType)
    {
        if (string.IsNullOrEmpty(value))
            return DBNull.Value;

        return postgresType.ToLower() switch
        {
            "smallint" => short.Parse(value),
            "integer" or "serial" => int.Parse(value),
            "bigint" or "bigserial" => long.Parse(value),
            "real" => float.Parse(value),
            "double precision" => double.Parse(value),
            "numeric" or "decimal" => decimal.Parse(value),
            "boolean" => bool.Parse(value),
            "uuid" => Guid.Parse(value),
            _ => value,
        };
    }

    private static object ConvertPostgresValue(string postgresType, object value)
    {
        if (value is null || value == DBNull.Value)
            return null;

        return postgresType.ToLower() switch
        {
            "smallint" => Convert.ToInt16(value),
            "integer" or "serial" => Convert.ToInt32(value),
            "bigint" or "bigserial" => Convert.ToInt64(value),
            "real" => Convert.ToSingle(value),
            "double precision" => Convert.ToDouble(value),
            "numeric" or "decimal" => Convert.ToDecimal(value),
            "boolean" => Convert.ToBoolean(value),
            "date" or "timestamp" or "timestamptz" => Convert.ToDateTime(value),
            "uuid" => value.ToString(),
            "bytea" => value,
            "json" or "jsonb" => value.ToString(),
            _ => value,
        };
    }

    private class TableInfo
    {
        public string Schema { get; init; }
        public string TableName { get; init; }
        public List<string> PrimaryKeyColumns { get; init; }
        public string FullName => $"{Schema}.{TableName}";
    }
}
