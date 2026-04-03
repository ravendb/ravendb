using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Npgsql.Replication;
using Pgvector.Npgsql;
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
    private readonly NpgsqlDataSource _dataSource;
    private string _publicationName;
    private string _slotName;

    // Cache resolved type categories per RelationMessage OID layout.
    // Column types are fixed per relation in the replication stream.
    private readonly Dictionary<string, PostgresTypeCategory[]> _relationTypeCache = new();

    public PostgresCdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
        : base(configuration, database)
    {
        _documentProcessor = new CdcSinkDocumentProcessor(configuration) { Logger = Logger };
        _connectionString = configuration.Connection.ConnectionString;

        var dataSourceBuilder = new NpgsqlDataSourceBuilder(_connectionString);
        dataSourceBuilder.UseVector();
        _dataSource = dataSourceBuilder.Build();
    }

    protected override async Task RunInternalAsync(CancellationToken ct)
    {
        await EnsureReplicationSetup(ct);
        await EnsureReplicaIdentityForEmbeddedTables(ct);
        await HandleInitialLoad(ct);
        _initialLoadTcs.TrySetResult();
        await StartListening(ct);
    }

    private async Task EnsureReplicationSetup(CancellationToken ct)
    {
        var tableNames = Configuration.CollectAllSourceTableNames("public");

        _publicationName = Configuration.Postgres.PublicationName;
        _slotName = Configuration.Postgres.SlotName;

        await using var conn = _dataSource.CreateConnection();
        await conn.OpenAsync(ct);

        // --- Publication: create if missing, verify table coverage if exists ---
        bool publicationExists;
        await using (var cmd = new NpgsqlCommand("SELECT 1 FROM pg_publication WHERE pubname = @pubName", conn))
        {
            cmd.Parameters.AddWithValue("pubName", _publicationName);
            publicationExists = await cmd.ExecuteScalarAsync(ct) != null;
        }

        if (publicationExists)
        {
            await VerifyPublicationTableCoverage(conn, tableNames, ct);
        }
        else
        {
            var tableList = string.Join(", ", tableNames);

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

        // --- Slot: verify if exists, create if not ---
        await using (var cmd = new NpgsqlCommand(
            "SELECT plugin FROM pg_replication_slots WHERE slot_name = @slotName", conn))
        {
            cmd.Parameters.AddWithValue("slotName", _slotName);
            var plugin = (string)await cmd.ExecuteScalarAsync(ct);

            if (plugin != null)
            {
                if (string.Equals(plugin, "pgoutput", StringComparison.OrdinalIgnoreCase) == false)
                {
                    throw new InvalidOperationException(
                        $"Replication slot '{_slotName}' exists but uses plugin '{plugin}' instead of 'pgoutput'. " +
                        $"CDC Sink requires the pgoutput plugin. Drop the existing slot and let the task recreate it, " +
                        $"or create a new slot manually: SELECT pg_create_logical_replication_slot('{_slotName}', 'pgoutput');");
                }
            }
            else
            {
                try
                {
                    await using var createCmd = new NpgsqlCommand(
                        $"SELECT pg_create_logical_replication_slot('{_slotName}', 'pgoutput')", conn);
                    await createCmd.ExecuteNonQueryAsync(ct);
                }
                catch (PostgresException ex) when (ex.SqlState == "42710")
                {
                    // Race condition: slot was created between our check and create
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
    }

    private async Task VerifyPublicationTableCoverage(NpgsqlConnection conn, List<string> configuredTableNames, CancellationToken ct)
    {
        var publishedTables = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        await using (var cmd = new NpgsqlCommand("""
            SELECT n.nspname || '.' || c.relname
            FROM pg_publication_rel pr
            JOIN pg_class c ON c.oid = pr.prrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE pr.prpubid = (SELECT oid FROM pg_publication WHERE pubname = @pubName)
            """, conn))
        {
            cmd.Parameters.AddWithValue("pubName", _publicationName);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                publishedTables.Add(reader.GetString(0));
        }

        // Check for configured tables missing from the publication
        var missing = new List<string>();
        foreach (var table in configuredTableNames)
        {
            if (publishedTables.Contains(table) == false)
                missing.Add(table);
        }

        if (missing.Count > 0)
        {
            var tableList = string.Join(", ", missing);
            try
            {
                await using var alterCmd = new NpgsqlCommand(
                    $"ALTER PUBLICATION {_publicationName} ADD TABLE {tableList}", conn);
                await alterCmd.ExecuteNonQueryAsync(ct);

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] Added missing tables to publication '{_publicationName}': {tableList}");
            }
            catch (PostgresException ex)
            {
                throw new InvalidOperationException(
                    $"Publication '{_publicationName}' does not include tables: {tableList}. " +
                    $"Attempted to add them automatically but failed ({ex.MessageText}). " +
                    $"Ask a database administrator to run: ALTER PUBLICATION {_publicationName} ADD TABLE {tableList};", ex);
            }
        }

        // Warn about extra tables in the publication that aren't in the configuration
        var extra = new List<string>();
        var configuredSet = new HashSet<string>(configuredTableNames, StringComparer.OrdinalIgnoreCase);
        foreach (var table in publishedTables)
        {
            if (configuredSet.Contains(table) == false)
                extra.Add(table);
        }

        if (extra.Count > 0)
        {
            var alert = AlertRaised.Create(
                Database.Name, Tag,
                $"Publication '{_publicationName}' includes tables not configured in the CDC Sink task: {string.Join(", ", extra)}. " +
                "Rows from these tables will be discarded. Consider narrowing the publication to only the configured tables.",
                AlertReason.CdcSink_Error,
                NotificationSeverity.Warning,
                key: $"{Tag}/{Name}/publication-extra-tables");

            Database.NotificationCenter.Add(alert);
        }
    }

    /// <summary>
    /// Ensures that embedded tables have an appropriate REPLICA IDENTITY for CDC DELETE routing.
    ///
    /// PostgreSQL's default REPLICA IDENTITY only sends primary key columns on DELETE.
    /// For embedded tables, the CDC processor needs the join column (FK to the parent table)
    /// to route the delete to the correct parent document. If the join column is already part
    /// of the PK, the default identity is sufficient. Otherwise, we need REPLICA IDENTITY FULL
    /// so that all columns (including the join column) are sent on DELETE events.
    ///
    /// When OnDelete.IgnoreDeletes is set on the embedded table config, we skip this check entirely
    /// since DELETE events are discarded and routing is not needed.
    ///
    /// Example:
    ///   order_lines(id PK, order_id FK, product, quantity)
    ///   JoinColumns = ["order_id"]  — order_id is NOT in the PK
    ///   → We set REPLICA IDENTITY FULL so DELETE events include order_id
    ///
    ///   order_lines(order_id, line_num, product) PK(order_id, line_num)
    ///   JoinColumns = ["order_id"]  — order_id IS in the PK
    ///   → Default REPLICA IDENTITY is sufficient, no action needed
    /// </summary>
    private async Task EnsureReplicaIdentityForEmbeddedTables(CancellationToken ct)
    {
        var embeddedTables = CollectEmbeddedTablesNeedingReplicaIdentity(Configuration.Tables);

        if (embeddedTables.Count == 0)
            return;

        await using var conn = _dataSource.CreateConnection();
        await conn.OpenAsync(ct);

        foreach (var embedded in embeddedTables)
        {
            var schema = embedded.SourceTableSchema ?? "public";
            var table = embedded.SourceTableName;

            var requiredColumns = new HashSet<string>(embedded.JoinColumns, StringComparer.OrdinalIgnoreCase);
            foreach (var pk in embedded.PrimaryKeyColumns)
                requiredColumns.Add(pk);

            var error = await CdcSinkSourceVerifier.CheckReplicaIdentityCoversColumns(conn, schema, table, requiredColumns);
            if (error == null)
                continue;

            // Replica identity is insufficient — set to FULL so DELETE events include join columns
            try
            {
                await using var alterCmd = new NpgsqlCommand(
                    $"ALTER TABLE {schema}.{table} REPLICA IDENTITY FULL", conn);
                await alterCmd.ExecuteNonQueryAsync(ct);

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] Set REPLICA IDENTITY FULL on {schema}.{table} " +
                        $"(join columns {string.Join(", ", embedded.JoinColumns)} are not in the primary key)");
            }
            catch (PostgresException ex) when (ex.SqlState == "42501")
            {
                throw new InvalidOperationException(
                    $"Insufficient permissions to set REPLICA IDENTITY FULL on '{schema}.{table}'. " +
                    $"The embedded table's join column(s) ({string.Join(", ", embedded.JoinColumns)}) are not part of " +
                    $"the primary key, so DELETE events need REPLICA IDENTITY FULL to include the join columns " +
                    $"for routing to the parent document. An administrator can run:\n\n" +
                    $"  ALTER TABLE {schema}.{table} REPLICA IDENTITY FULL;\n\n" +
                    $"Alternatively, set OnDelete.IgnoreDeletes = true on this embedded table to skip delete processing.\n\n" +
                    $"PostgreSQL error: {ex.MessageText}", ex);
            }
        }
    }

    private static List<CdcSinkEmbeddedTableConfig> CollectEmbeddedTablesNeedingReplicaIdentity(
        List<CdcSinkTableConfig> rootTables)
    {
        var result = new List<CdcSinkEmbeddedTableConfig>();
        foreach (var root in rootTables)
        {
            CdcSinkConfiguration.ForEachEmbeddedTable(root.EmbeddedTables, e =>
            {
                if (e.OnDelete?.IgnoreDeletes == true)
                    return;

                foreach (var joinCol in e.JoinColumns)
                {
                    if (e.PrimaryKeyColumns.Contains(joinCol) == false)
                    {
                        result.Add(e);
                        return;
                    }
                }
            });
        }
        return result;
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

        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext jsonParsingContext))
        {

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

                    // PostgreSQL retains WAL segments until the replication slot confirms receipt.
                    // We must ack promptly after processing rows so WAL doesn't accumulate,
                    // but avoid re-acking the same LSN on an idle stream. The rowsSinceLastAck
                    // guard ensures we ack exactly once after a batch, then stay silent until
                    // new data arrives.
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
                    AddIfNotNull(pending, await DecodeRow(insert.Relation, insert.NewRow, CdcSinkOperation.Upsert, jsonParsingContext));
                    break;
                case UpdateMessage update:
                    AddIfNotNull(pending, await DecodeRow(update.Relation, update.NewRow, CdcSinkOperation.Upsert, jsonParsingContext));
                    break;
                case KeyDeleteMessage keyDel:
                    AddIfNotNull(pending, await DecodeRow(keyDel.Relation, keyDel.Key, CdcSinkOperation.Delete, jsonParsingContext));
                    break;
                case FullDeleteMessage fullDel:
                    AddIfNotNull(pending, await DecodeRow(fullDel.Relation, fullDel.OldRow, CdcSinkOperation.Delete, jsonParsingContext));
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
    }

    private async Task<CdcSinkDocumentOp> DecodeRow(
        RelationMessage relation, ReplicationTuple row, CdcSinkOperation operation, JsonOperationContext jsonParsingContext)
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

        return _documentProcessor.ProcessRow(cdcRow, jsonParsingContext);
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
            1082 => PostgresTypeCategory.DateOnly,              // date
            1114 or 1184 => PostgresTypeCategory.DateTime,      // timestamp, timestamptz
            2950 => PostgresTypeCategory.Uuid,                  // uuid
            17 => PostgresTypeCategory.Bytea,                   // bytea
            114 or 3802 => PostgresTypeCategory.Json,           // json, jsonb
            // Extension types (e.g., pgvector) have dynamically assigned OIDs and fall
            // through to Other, which reads them as text. For pgvector, the text
            // representation (e.g., "[1,2,3]") can be stored as a JSON column or
            // converted to an attachment via CdcColumnType configuration.
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
        DateOnly,
        DateTime,
        Uuid,
        Bytea,
        Json
    }

    private async Task SubmitBatch(List<CdcSinkDocumentOp> ops, string lastLsn = null,
        Dictionary<string, CdcSinkTableLoadState> tableLoadUpdates = null)
    {
        var command = new CdcSinkBatchCommand(
            Database, ops, Configuration.Name, lastLsn,
            tableLoadUpdates: tableLoadUpdates,
            patchRequest: _documentProcessor.CombinedPatchRequest,
            statsScope: null, statistics: Statistics, logger: Logger);

        await Database.TxMerger.Enqueue(command);

        Database.CdcSinkLoader.OnBatchCompleted(Configuration.Name, Name, Statistics);
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

        if (Configuration.SkipInitialLoad)
        {
            var updates = new Dictionary<string, CdcSinkTableLoadState>();
            foreach (var tableInfo in allTables)
            {
                var tableKey = tableInfo.FullName;
                if (state.Tables.TryGetValue(tableKey, out var ts) && ts.InitialLoadCompleted)
                    continue;
                updates[tableKey] = new CdcSinkTableLoadState { InitialLoadCompleted = true };
            }

            if (updates.Count > 0)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] SkipInitialLoad is set — marking {updates.Count} table(s) as complete without scanning.");
                await SubmitBatch([], tableLoadUpdates: updates);
            }

            return;
        }

        foreach (var tableInfo in allTables)
        {
            var tableKey = tableInfo.FullName;

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
        await using var conn = _dataSource.CreateConnection();
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
                return;
            }

            await lastBatch;

            var tableLoadUpdate = new Dictionary<string, CdcSinkTableLoadState>
            {
                [tableKey] = new CdcSinkTableLoadState { LastKeyValues = [.. newLastKeys] }
            };

            lastBatch = SubmitBatch(ops, tableLoadUpdates: tableLoadUpdate);
            lastKeys = newLastKeys;
        }
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
            using var __ = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext jsonParsingCtx);

            var ops = new List<CdcSinkDocumentOp>();

            while (await reader.ReadAsync(ct))
            {
                var data = new Dictionary<string, object>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var name = reader.GetName(i);
                    var dataTypeName = reader.GetDataTypeName(i);
                    if (dataTypeName == "-.-")
                        throw new InvalidOperationException(
                            $"Column '{name}' in table '{tableInfo.FullName}' has an unmapped Postgres extension type " +
                            $"that cannot be read during initial load. You should exclude this column from the CDC Sink " +
                            $"column mappings or report this as an issue.");

                    var value = reader.IsDBNull(i) ? null : NormalizeReaderValue(reader.GetValue(i));
                    data[name] = value;
                }

                var row = new CdcSinkRow
                {
                    TableSchema = tableInfo.Schema,
                    TableName = tableInfo.TableName,
                    Operation = CdcSinkOperation.Upsert,
                    Data = data,
                };

                var op = _documentProcessor.ProcessRow(row, jsonParsingCtx);
                if (op != null)
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

    /// <summary>
    /// Normalizes raw CLR values from <see cref="System.Data.Common.DbDataReader.GetValue"/> during
    /// initial load to match the types produced by <see cref="ConvertPostgresValue"/> during CDC
    /// streaming.
    /// </summary>
    private static object NormalizeReaderValue(object value)
    {
        return value switch
        {
            null or DBNull => null,
            DateOnly => value,
            DateTimeOffset dto => dto.UtcDateTime,             // DateTimeOffset → DateTime UTC
            byte or short or int => Convert.ToInt64(value),    // small ints → long
            float f => (double)f,                              // float → double
            decimal d => (double)d,                            // decimal → double
            Guid g => g.ToString(),                            // Guid → string
            Pgvector.Vector v => v.ToArray(),                     // pgvector → float[]
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
            PostgresTypeCategory.DateOnly => System.DateOnly.FromDateTime(Convert.ToDateTime(value)),
            PostgresTypeCategory.DateTime => Convert.ToDateTime(value),
            PostgresTypeCategory.Uuid => value.ToString(),
            PostgresTypeCategory.Bytea => value,
            PostgresTypeCategory.Json => value.ToString(),
            _ => value,
        };
    }

    private static void AddIfNotNull(List<CdcSinkDocumentOp> list, CdcSinkDocumentOp op)
    {
        if (op != null)
            list.Add(op);
    }
}
