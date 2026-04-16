using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Data.Common;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.NotificationCenter.Notifications;
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
    private readonly string _connectionString;
    private readonly string _replicationConnectionString;
    private readonly TimeSpan _replicationTimeout;
    private readonly NpgsqlDataSource _dataSource;
    private string _publicationName;
    private string _slotName;
    private uint _vectorOid = uint.MaxValue; // pgvector extension OID, resolved at setup time. MaxValue = not installed.

    /// <summary>
    /// Lightweight per-relation state: type categories for value decoding and the processor
    /// for column mapping. Keyed by the real (un-sentinel'd) RelationId.
    /// Rebuilt when Npgsql calls Populate() on the RelationMessage (schema change).
    /// </summary>
    private readonly Dictionary<uint, (PostgresTypeCategory[] Types, CdcSinkTableProcessor Processor)> _relationProcessors = new();

    /// <summary>
    /// Schema change detection via RelationId sentinel bit.
    ///
    /// Npgsql reuses the same RelationMessage object per table and calls Populate() to update
    /// it in-place only when PostgreSQL sends a new Relation message (schema change or first
    /// encounter). We exploit this:
    ///
    /// After processing a RelationMessage, we flip the high bit of RelationId via reflection.
    /// On subsequent rows, if the high bit is set, the schema hasn't changed (fast path).
    /// If Populate() was called (schema change), it overwrites RelationId with the real value
    /// (high bit clear) — we detect this and rebuild the column mapping.
    ///
    /// Cost: one bitwise AND per row on the hot path. Rebuild only on actual schema changes.
    /// </summary>
    private const uint RelationIdSentinelBit = 0x80000000;
    private static readonly FieldInfo RelationIdBackingField =
        typeof(RelationMessage).GetField("<RelationId>k__BackingField", BindingFlags.NonPublic | BindingFlags.Instance)
        ?? throw new InvalidOperationException(
            "Cannot find RelationMessage.RelationId backing field. " +
            "This may indicate an incompatible Npgsql version.");

    private System.Text.StringBuilder _reusableSb;

    public PostgresCdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
        : base(configuration, database, "public")
    {
        _connectionString = configuration.Connection.ConnectionString;
        _replicationTimeout = Database.Configuration.CdcSink.PostgresReplicationTimeout.AsTimeSpan;

        var csb = new NpgsqlConnectionStringBuilder(_connectionString);
        var walSenderMs = (int)_replicationTimeout.TotalMilliseconds;
        csb.Options = string.IsNullOrEmpty(csb.Options)
            ? $"-c wal_sender_timeout={walSenderMs}"
            : $"{csb.Options} -c wal_sender_timeout={walSenderMs}";
        _replicationConnectionString = csb.ToString();

        var dataSourceBuilder = new NpgsqlDataSourceBuilder(_connectionString);
        dataSourceBuilder.UseVector();
        _dataSource = dataSourceBuilder.Build();
    }

    public override void Dispose()
    {
        base.Dispose();

        // Ensure the replication connection is closed even if the async iterator
        // didn't dispose cleanly (e.g., due to sync-over-async in Stop()).
        // Without this, the WAL sender may keep the slot active, preventing
        // database drops in test scenarios.
        try
        {
            _replicationConn?.DisposeAsync().AsTask().Wait(TimeSpan.FromSeconds(5));
        }
        catch
        {
            // best effort — the connection may already be disposed by the iterator
        }

        _dataSource.Dispose();
    }

    protected override async Task RunInternalAsync(CancellationToken ct)
    {
        // in case of error, we'll re-learn the schema (it may have changed).
        _columnTypesCache.Clear();
        _relationProcessors.Clear();
        await EnsureReplicationSetup(ct);
        await EnsureReplicaIdentityForEmbeddedTables(ct);
        await HandleInitialLoad(ct);
        _initialLoadTcs.TrySetResult();
        await ProcessCdcStream(ct);
    }

    protected override async Task<string> ReadCurrentCheckpointAsync(CancellationToken ct)
    {
        await using var conn = await _dataSource.OpenConnectionAsync(ct);
        await using var cmd = new NpgsqlCommand(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = @slotName", conn);
        cmd.Parameters.AddWithValue("slotName", _slotName);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result?.ToString();
    }

    private async Task EnsureReplicationSetup(CancellationToken ct)
    {
        var allTables = Configuration.CollectAllTablesFlat("public");

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
            await VerifyPublicationTableCoverage(conn, allTables, ct);
        }
        else
        {
            var quotedTableList = QuoteTableList(allTables);

            try
            {
                var quotedPubName = CommandBuilder.QuoteIdentifier(_publicationName);
                await using var createCmd = new NpgsqlCommand(
                    $"CREATE PUBLICATION {quotedPubName} FOR TABLE {quotedTableList}", conn);
                await createCmd.ExecuteNonQueryAsync(ct);
            }
            catch (PostgresException ex) when (ex.SqlState == "42501")
            {
                var quotedPubName = CommandBuilder.QuoteIdentifier(_publicationName);
                throw new InvalidOperationException(
                    $"""
                    Insufficient permissions to create publication '{_publicationName}'.
                    The database user must have CREATE permission on the database, or an administrator can create the publication manually:

                      CREATE PUBLICATION {quotedPubName} FOR TABLE {quotedTableList};

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
                        $"""
                        Replication slot '{_slotName}' exists but uses plugin '{plugin}' instead of 'pgoutput'. 
                        CDC Sink requires the 'pgoutput' plugin to function.
                        Drop the existing slot and let the task recreate it, or create a new slot manually: 
                        
                            SELECT pg_create_logical_replication_slot('{_slotName}', 'pgoutput');
                        """);
                }
            }
            else
            {
                try
                {
                    await using var createCmd = new NpgsqlCommand(
                        $"SELECT pg_create_logical_replication_slot(@slotName, 'pgoutput')", conn);
                    createCmd.Parameters.AddWithValue("slotName", _slotName);
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
                        Insufficient permissions to create replication slot '{_slotName}'. 
                        The database user must have the REPLICATION role attribute, or an administrator can create the slot manually:

                          SELECT pg_create_logical_replication_slot('{_slotName}', 'pgoutput');

                        PostgreSQL error: {ex.MessageText}
                        """, ex);
                }
            }
        }

        // If a previous consumer (e.g., a process that was stopped but whose WAL sender
        // hasn't timed out yet) is still holding the slot, terminate it so we can connect.
        // PostgreSQL's default wal_sender_timeout is 60 seconds; without this, we'd have
        // to wait for it to expire before the new process can stream.
        await using (var cmd = new NpgsqlCommand(
            "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = @slotName AND active_pid IS NOT NULL",
            conn))
        {
            cmd.Parameters.AddWithValue("slotName", _slotName);
            await cmd.ExecuteScalarAsync(ct);
        }

        // Resolve pgvector extension OID so OidToCategory can recognize vector columns
        // in the CDC stream. Returns null if the extension is not installed.
        await using (var cmd = new NpgsqlCommand("SELECT oid FROM pg_type WHERE typname = 'vector'", conn))
        {
            var result = await cmd.ExecuteScalarAsync(ct);
            _vectorOid = result is uint oid ? oid : uint.MaxValue;
        }
    }

    private async Task VerifyPublicationTableCoverage(NpgsqlConnection conn, List<CdcSinkConfiguration.TableInfo> configuredTables, CancellationToken ct)
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
        var missing = new List<CdcSinkConfiguration.TableInfo>();
        foreach (var table in configuredTables)
        {
            if (publishedTables.Contains(table.FullName) == false)
                missing.Add(table);
        }

        if (missing.Count > 0)
        {
            var quotedMissing = QuoteTableList(missing);
            var quotedPubName = CommandBuilder.QuoteIdentifier(_publicationName);
            try
            {
                await using var alterCmd = new NpgsqlCommand(
                    $"ALTER PUBLICATION {quotedPubName} ADD TABLE {quotedMissing}", conn);
                await alterCmd.ExecuteNonQueryAsync(ct);

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] Added missing tables to publication '{_publicationName}': {quotedMissing}");
            }
            catch (PostgresException ex)
            {
                throw new InvalidOperationException(
                    $"""
                    Publication '{_publicationName}' does not include tables: {quotedMissing}. Attempted to add them automatically but failed ({ex.MessageText}). Ask a database administrator to run: ALTER PUBLICATION {quotedPubName} ADD TABLE {quotedMissing};
                    """, ex);
            }
        }

        // Warn about extra tables in the publication that aren't in the configuration
        var extra = new List<string>();
        var configuredSet = new HashSet<string>(configuredTables.Select(t => t.FullName), StringComparer.OrdinalIgnoreCase);
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
            var quotedTable = $"{CommandBuilder.QuoteIdentifier(schema)}.{CommandBuilder.QuoteIdentifier(table)}";
            try
            {
                await using var alterCmd = new NpgsqlCommand(
                    $"ALTER TABLE {quotedTable} REPLICA IDENTITY FULL", conn);
                await alterCmd.ExecuteNonQueryAsync(ct);

                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] Set REPLICA IDENTITY FULL on {schema}.{table} " +
                        $"(join columns {string.Join(", ", embedded.JoinColumns)} are not in the primary key)");
            }
            catch (PostgresException ex) when (ex.SqlState == "42501")
            {
                throw new InvalidOperationException(
                    $"""
                    Insufficient permissions to set REPLICA IDENTITY FULL on '{schema}.{table}'. 
                    The embedded table's join column(s) ({string.Join(", ", embedded.JoinColumns)}) are not part of the primary key.
                    DELETE events need REPLICA IDENTITY FULL to include the join columns for routing to the parent document. 
                    An administrator can run:

                      ALTER TABLE {quotedTable} REPLICA IDENTITY FULL;

                    Alternatively, set OnDelete.IgnoreDeletes = true on this embedded table to skip delete processing.

                    PostgreSQL error: {ex.MessageText}
                    """, ex);
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

    private LogicalReplicationConnection _replicationConn;
    private NpgsqlTypes.NpgsqlLogSequenceNumber _lastLsn;
    private int _rowsSinceLastAck;

    protected override async IAsyncEnumerable<CdcEvent> GetCdcEvents([EnumeratorCancellation] CancellationToken ct)
    {
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var state = LoadState(context);
            _lastLsn = string.IsNullOrEmpty(state.LastLsn)
                ? new NpgsqlTypes.NpgsqlLogSequenceNumber(0)
                : NpgsqlTypes.NpgsqlLogSequenceNumber.Parse(state.LastLsn);
        }

        await VerifyNoGapsInLsn(ct);

        _rowsSinceLastAck = 0;
        await using var conn = new LogicalReplicationConnection(_replicationConnectionString);
        conn.WalReceiverTimeout = _replicationTimeout;
        await conn.Open(ct);
        _replicationConn = conn;

        var replicationStream = conn.StartReplication(
            new PgOutputReplicationSlot(_slotName),
            new PgOutputReplicationOptions(_publicationName, PgOutputProtocolVersion.V1),
            ct,
            _lastLsn);

        await foreach (var message in replicationStream.WithCancellation(ct))
        {
            LastActivityTime = Database.Time.GetUtcNow();

            switch (message)
            {
                case InsertMessage insert:
                    yield return new CdcEvent(CdcEventType.Upsert,
                        await DecodeRow(insert.Relation, insert.NewRow, CdcSinkOperation.Upsert, StreamingJsonContext, ct), null);
                    break;
                case FullUpdateMessage fullUpdate:
                {
                    // REPLICA IDENTITY FULL — old row available for reparent detection
                    var newOp = await DecodeRow(fullUpdate.Relation, fullUpdate.NewRow, CdcSinkOperation.Upsert, StreamingJsonContext, ct);
                    if (newOp?.Processor is { IsRoot: false })
                    {
                        var (_, oldValues) = await DecodeRowInternal(fullUpdate.Relation, fullUpdate.OldRow, ct);
                        foreach (var evt in CreateEmbeddedUpdateEvents(newOp, oldValues))
                            yield return evt;
                    }
                    else
                    {
                        yield return new CdcEvent(CdcEventType.Upsert, newOp, null);
                    }
                    break;
                }
                case UpdateMessage update:
                    yield return new CdcEvent(CdcEventType.Upsert,
                        await DecodeRow(update.Relation, update.NewRow, CdcSinkOperation.Upsert, StreamingJsonContext, ct), null);
                    break;
                case KeyDeleteMessage keyDel:
                    yield return new CdcEvent(CdcEventType.Delete,
                        await DecodeRow(keyDel.Relation, keyDel.Key, CdcSinkOperation.Delete, StreamingJsonContext, ct), null);
                    break;
                case FullDeleteMessage fullDel:
                    yield return new CdcEvent(CdcEventType.Delete,
                        await DecodeRow(fullDel.Relation, fullDel.OldRow, CdcSinkOperation.Delete, StreamingJsonContext, ct), null);
                    break;
                case CommitMessage commit:
                    _lastLsn = commit.CommitLsn;
                    yield return new CdcEvent(CdcEventType.TransactionCommit, null, commit.CommitLsn.ToString());
                    break;
            }
        }
    }

    private async Task VerifyNoGapsInLsn(CancellationToken ct)
    {
        if (_lastLsn <= new NpgsqlTypes.NpgsqlLogSequenceNumber(0))
            return;

        // Detect stale LSN: if the slot was recreated (e.g., after restore), its restart_lsn
        // will be ahead of our saved position. Changes in the gap are permanently lost.
        await using var checkConn = _dataSource.CreateConnection();
        await checkConn.OpenAsync(ct);
        await using var checkCmd = new NpgsqlCommand(
            "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = @slotName", checkConn);
        checkCmd.Parameters.AddWithValue("slotName", _slotName);
        var slotLsn = await checkCmd.ExecuteScalarAsync(ct);
        if (slotLsn is not NpgsqlTypes.NpgsqlLogSequenceNumber restartLsn || restartLsn <= _lastLsn)
            return;

        var msg = $"[{Name}] Replication slot '{_slotName}' restart LSN ({restartLsn}) is ahead of " +
                  $"our saved position ({_lastLsn}). Changes between these positions are lost. " +
                  "This can happen when the slot is recreated after a backup restore. " +
                  "Consider resetting the task state to trigger a full initial reload.";

        if (Logger.IsErrorEnabled)
            Logger.Error(msg);

        Database.NotificationCenter.Add(AlertRaised.Create(
            Database.Name, Tag, msg, AlertReason.CdcSink_Error,
            NotificationSeverity.Warning, key: $"{Tag}/{Name}/stale-lsn"));
    }

    protected override async Task OnBatchFlushed(string checkpoint, int rows)
    {
        // PostgreSQL retains WAL segments until the replication slot confirms receipt.
        // We ack periodically (every maxBatchSize rows) to balance WAL retention against
        // protocol overhead.
        _rowsSinceLastAck += rows;
        _replicationConn.SetReplicationStatus(_lastLsn);
        // If rows is 0, it means the batch was empty (e.g., a transaction with no relevant changes, or all changes were filtered out).
        // Even in this case, we want to ack the LSN to advance the replication slot and allow WAL cleanup, otherwise a stream of 
        // empty transactions could stall the slot indefinitely.
        if (rows is 0 || _rowsSinceLastAck >= Database.Configuration.CdcSink.MaxBatchSize)
        {
            await _replicationConn.SendStatusUpdate(CancellationToken);
            _rowsSinceLastAck = 0;
        }
    }

    private async Task<CdcSinkDocumentOp> DecodeRow(
        RelationMessage relation, ReplicationTuple row, CdcSinkOperation operation, JsonOperationContext jsonParsingContext, CancellationToken ct = default)
    {
        var (proc, values) = await DecodeRowInternal(relation, row, ct);
        return DocumentProcessor.ProcessRow(proc, operation, values, jsonParsingContext);
    }

    /// <summary>
    /// Resolves the processor for a relation, decodes a replication tuple into raw column values,
    /// and returns both. Used by <see cref="DecodeRow"/> and by the reparent detection path
    /// (which needs the raw values without calling ProcessRow).
    /// </summary>
    private async Task<(CdcSinkTableProcessor Processor, object[] Values)> DecodeRowInternal(
        RelationMessage relation, ReplicationTuple row, CancellationToken ct)
    {
        var relationId = relation.RelationId;

        if ((relationId & RelationIdSentinelBit) == 0)
        {
            // RelationId doesn't have our sentinel bit — either first time seeing this relation,
            // or Npgsql called Populate() because PostgreSQL sent a new RelationMessage (schema change).
            // (Re)build the column mapping from the current RelationMessage contents.
            var realId = relationId;

            var typeCategories = BuildTypeCategoriesFromRelation(relation);

            var columnNames = new string[relation.Columns.Count];
            for (int i = 0; i < relation.Columns.Count; i++)
                columnNames[i] = relation.Columns[i].ColumnName;

            var processor = DocumentProcessor.GetProcessor(relation.Namespace, relation.RelationName);
            processor.SetSourceColumnNames(columnNames);

            _relationProcessors[realId] = (typeCategories, processor);

            // Set the sentinel bit via reflection so subsequent rows skip this rebuild.
            // Npgsql's Populate() will overwrite RelationId with the real value on schema change,
            // clearing our sentinel and triggering a rebuild on the next row.
            RelationIdBackingField.SetValue(relation, realId | RelationIdSentinelBit);
        }

        var (types, proc) = _relationProcessors[relationId & ~RelationIdSentinelBit];

        var values = proc.RentValues();
        int columnIndex = 0;
        var relationKey = $"{relation.Namespace}.{relation.RelationName}";

        await foreach (var item in row)
        {
            var value = item.IsDBNull ? null : await item.Get(ct);
            values[columnIndex] = ConvertPostgresValue(types[columnIndex], value, relationKey, item.GetFieldName());
            columnIndex++;
        }

        return (proc, values);
    }

    /// <summary>
    /// Build type category array from the RelationMessage's column OIDs.
    /// PostgreSQL OIDs are well-known and documented.
    /// </summary>
    private PostgresTypeCategory[] BuildTypeCategoriesFromRelation(RelationMessage relation)
    {
        var categories = new PostgresTypeCategory[relation.Columns.Count];
        for (int i = 0; i < relation.Columns.Count; i++)
        {
            categories[i] = OidToCategory(relation.Columns[i].DataTypeId);
        }
        return categories;
    }

    private PostgresTypeCategory OidToCategory(uint oid)
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
            // Array types — Postgres has a dedicated OID for each base type's array form.
            // pgoutput delivers these as text literals like "{tag1,tag2,tag3}".
            1000 or 1001 or 1005 or 1007 or 1009 or 1015 or 1016
                or 1021 or 1022 or 1028 or 1231 or 2951 or 199 or 3807
                => PostgresTypeCategory.TextArray,              // bool[], bytea[], int2[], int4[], text[], varchar[], int8[], float4[], float8[], oid[], numeric[], uuid[], json[], jsonb[]
            _ when oid == _vectorOid => PostgresTypeCategory.Vector,
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
        Json,
        TextArray,
        Vector
    }

    private string QuoteTableList(List<CdcSinkConfiguration.TableInfo> tables)
    {
        return string.Join(", ", tables.Select(t =>
            $"{CommandBuilder.QuoteIdentifier(t.Schema)}.{CommandBuilder.QuoteIdentifier(t.TableName)}"));
    }

    protected override DbCommandBuilder CommandBuilder { get; } = new NpgsqlCommandBuilder();

    /// <summary>
    /// PostgreSQL keepalives are handled internally by Npgsql at the protocol level and
    /// do NOT surface as messages in the replication stream. LastActivityTime only updates
    /// on actual data messages (Insert, Update, Delete, Commit, Relation, etc.).
    ///
    /// If the connection dies, WalReceiverTimeout (configured via CdcSink.Postgres.ReplicationTimeoutInSec)
    /// will throw an exception, which triggers fallback mode. The server-side
    /// wal_sender_timeout is set to match, so keepalives arrive at roughly half that interval.
    /// Therefore:
    ///   - FallbackTime set → connection is dead, detected within ReplicationTimeout
    ///   - FallbackTime null + stale LastActivityTime → source is idle, connection is alive
    ///     (Npgsql would have thrown otherwise)
    ///
    /// We do NOT flag stale LastActivityTime as unhealthy for PostgreSQL because we have
    /// no way to distinguish "idle source" from "slow source" without protocol-level keepalive
    /// visibility. Npgsql handles that for us via WalReceiverTimeout.
    /// </summary>
    public override bool IsHealthy(out string issue)
    {
        issue = null;

        if (FallbackTime != null)
        {
            issue = $"Process is in error recovery (fallback mode). Last error: {Statistics.LastConsumeErrorTime:O}. " +
                    $"Next retry in: {FallbackTime.Value.TotalSeconds:F0}s.";
            return false;
        }

        // For PostgreSQL, if we're not in fallback mode, the connection is alive —
        // WalReceiverTimeout would have thrown and put us in fallback if the server
        // stopped responding. Stale LastActivityTime just means no data changes.
        return true;
    }

    protected override string GetDefaultSchema() => "public";


    protected override async Task<DbConnection> OpenInitialLoadConnection(CancellationToken ct)
    {
        var conn = _dataSource.CreateConnection();
        try
        {
            await conn.OpenAsync(ct);
            return conn;
        }
        catch
        {
            await conn.DisposeAsync();
            throw;
        }
    }



    private readonly Dictionary<string, Dictionary<string, string>> _columnTypesCache = new();

    protected override async Task BindKeysetParameters(DbCommand cmd, CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns, string[] lastKeys, CancellationToken ct)
    {
        var npgsqlCmd = (NpgsqlCommand)cmd;

        if (_columnTypesCache.TryGetValue(tableInfo.FullName, out var columnTypes) == false)
        {
            var conn = (NpgsqlConnection)cmd.Connection;
            columnTypes = await GetColumnTypes(conn, tableInfo.Schema, tableInfo.TableName, pkColumns, ct);
            _columnTypesCache[tableInfo.FullName] = columnTypes;
        }

        for (int i = 0; i < pkColumns.Count; i++)
        {
            var value = ConvertStringToType(lastKeys[i], columnTypes.GetValueOrDefault(pkColumns[i], "text"));
            npgsqlCmd.Parameters.AddWithValue($"k{i}", value);
        }
    }

    protected override object ConvertInitialLoadValue(DbDataReader reader, int ordinal)
    {
        var dataTypeName = reader.GetDataTypeName(ordinal);
        if (dataTypeName == "-.-")
            throw new InvalidOperationException(
                $"Column '{reader.GetName(ordinal)}' has an unmapped Postgres extension type " +
                $"that cannot be read during initial load. Exclude this column from the CDC Sink column mappings.");

        var rawValue = reader.GetValue(ordinal);
        // pgvector returns Pgvector.Vector which NormalizeForJson doesn't know about.
        return rawValue is Pgvector.Vector v ? (object)v.ToArray() : rawValue;
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
            "smallint" or "integer" or "serial" or "bigint" or "bigserial" => long.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            "real" or "double precision" => double.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            "numeric" or "decimal" => decimal.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            "boolean" => bool.Parse(value),
            "uuid" => Guid.Parse(value),
            _ => value,
        };
    }

    /// <summary>
    /// Normalizes PostgreSQL values for consistent storage in RavenDB.
    /// Note: PostgreSQL has no unsigned integer types, so ulong/uint are not handled here
    /// (unlike MySQL which has BIGINT UNSIGNED).
    /// </summary>
    private object ConvertPostgresValue(PostgresTypeCategory category, object value, string table, string column)
    {
        if (value is null || value == DBNull.Value)
            return null;

        return category switch
        {
            PostgresTypeCategory.Integer => Convert.ToInt64(value),
            PostgresTypeCategory.BigInt => Convert.ToInt64(value),
            // Float (float4/real): keep as float to preserve precision parity between
            // initial load (ADO.NET returns float) and CDC streaming (pgoutput text).
            PostgresTypeCategory.Float => value is float f ? f : Convert.ToSingle(value),
            PostgresTypeCategory.Double => Convert.ToDouble(value),
            // Numeric/decimal: keep as decimal to preserve exact representation (trailing
            // zeros, scale) consistently between initial load and CDC streaming.
            PostgresTypeCategory.Numeric => value is decimal d ? d : Convert.ToDecimal(value),
            // pgoutput text protocol sends booleans as "t"/"f" rather than "true"/"false",
            // which Convert.ToBoolean cannot parse. Handle all Postgres boolean forms.
            PostgresTypeCategory.Boolean => value is bool b ? b : ParsePostgresBoolean(value),
            // Npgsql 10+ returns DateOnly natively for date columns; earlier versions or
            // pgoutput text decoding may return DateTime or string — handle both.
            PostgresTypeCategory.DateOnly => value is DateOnly dateOnly ? dateOnly : DateOnly.FromDateTime(Convert.ToDateTime(value)),
            PostgresTypeCategory.DateTime => value is DateTime dt ? dt : DateTime.Parse(value.ToString(), System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind),
            PostgresTypeCategory.Uuid => value.ToString(),
            PostgresTypeCategory.Bytea => value,
            PostgresTypeCategory.Json => value.ToString(),
            PostgresTypeCategory.TextArray => ParsePostgresArrayLiteral(value.ToString()),
            PostgresTypeCategory.Vector => ParseVectorLiteral(value.ToString(), table, column),
            _ => value,
        };
    }

    /// <summary>
    /// Parses a PostgreSQL boolean value. The pgoutput text protocol sends "t"/"f"
    /// instead of "true"/"false", so Convert.ToBoolean fails. PostgreSQL also accepts
    /// "yes"/"no", "on"/"off", "1"/"0" in its boolean type.
    /// </summary>
    private static bool ParsePostgresBoolean(object value)
    {
        if (value is string s)
        {
            return s switch
            {
                "t" or "T" or "true" or "True" or "TRUE" or "yes" or "on" or "1" => true,
                "f" or "F" or "false" or "False" or "FALSE" or "no" or "off" or "0" => false,
                _ => Convert.ToBoolean(s),
            };
        }

        return Convert.ToBoolean(value);
    }

    /// <summary>
    /// Parses a pgvector text literal like "[0.1,0.2,0.3]" into a float[].
    /// </summary>
    private static float[] ParseVectorLiteral(string text, string table, string column)
    {
        if (string.IsNullOrEmpty(text) || text.Length < 3) // minimum "[0]"
            return [];

        var span = text.AsSpan();
        if (span[0] == '[' && span[span.Length - 1] == ']')
            span = span.Slice(1, span.Length - 2);

        // SIMD-accelerated comma count for pre-allocation
        int count = span.Count(',') + 1;
        var result = new float[count];
        int idx = 0;
        while (span.Length > 0)
        {
            int comma = span.IndexOf(',');
            var element = comma < 0 ? span : span.Slice(0, comma);

            if (float.TryParse(element, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out result[idx]) == false)
            {
                throw new FormatException(
                    $"Failed to parse pgvector element '{element}' at index {idx} in column '{column}' " +
                    $"of table '{table}'. Full value: '{text}'");
            }

            idx++;
            span = comma < 0 ? default : span.Slice(comma + 1);
        }

        return result;
    }

    /// <summary>
    /// Parses a Postgres array text literal like "{tag1,tag2,tag3}" into a string[].
    /// Handles quoted elements (e.g., {"hello, world","foo"}) and NULL elements.
    /// The pgoutput text protocol always delivers arrays in this format.
    /// </summary>
    private string[] ParsePostgresArrayLiteral(string text)
    {
        if (string.IsNullOrEmpty(text) || text == "{}")
            return [];

        var span = text.AsSpan();
        if (span[0] == '{' && span[span.Length - 1] == '}')
            span = span.Slice(1, span.Length - 2);

        // SIMD-accelerated check: no quotes means simple comma-separated values
        if (span.IndexOf('"') < 0)
        {
            int count = span.Count(',') + 1;
            var result = new string[count];
            int idx = 0;
            while (span.Length > 0)
            {
                int comma = span.IndexOf(',');
                var element = comma < 0 ? span : span.Slice(0, comma);
                result[idx++] = element.SequenceEqual("NULL") ? null : element.ToString();
                span = comma < 0 ? default : span.Slice(comma + 1);
            }
            return result;
        }

        // Slow path: quoted elements that may contain commas or escaped characters
        return ParsePostgresArrayLiteralQuoted(span);
    }

    private static readonly SearchValues<char> QuoteOrComma = SearchValues.Create("\",");
    private static readonly SearchValues<char> QuoteOrBackslash = SearchValues.Create("\"\\");

    private string[] ParsePostgresArrayLiteralQuoted(ReadOnlySpan<char> span)
    {
        var result = new List<string>();
        while (span.Length > 0)
        {
            if (span[0] == '"')
            {
                span = span.Slice(1); // skip opening quote
                // SIMD scan for closing quote or backslash escape
                int pos = span.IndexOfAny(QuoteOrBackslash);
                if (pos >= 0 && span[pos] == '"')
                {
                    // No escapes — common case
                    result.Add(span.Slice(0, pos).ToString());
                    span = span.Slice(pos + 1); // skip closing quote
                }
                else
                {
                    // Has escapes — build unescaped string
                    var sb = _reusableSb ??= new System.Text.StringBuilder();
                    sb.Clear();
                    while (span.Length > 0)
                    {
                        pos = span.IndexOfAny(QuoteOrBackslash);
                        if (pos < 0)
                            break;

                        sb.Append(span.Slice(0, pos));
                        if (span[pos] == '"')
                        {
                            span = span.Slice(pos + 1);
                            break;
                        }
                        // backslash escape: append the next char
                        if (pos + 1 < span.Length)
                            sb.Append(span[pos + 1]);
                        span = span.Slice(pos + 2);
                    }
                    result.Add(sb.ToString());
                }
            }
            else
            {
                // Unquoted element — SIMD scan for comma or quote (next element boundary)
                int comma = span.IndexOf(',');
                var element = comma < 0 ? span : span.Slice(0, comma);
                result.Add(element.SequenceEqual("NULL") ? null : element.ToString());
                span = comma < 0 ? default : span.Slice(comma + 1);
                continue;
            }

            // Skip comma separator after quoted element
            if (span.Length > 0 && span[0] == ',')
                span = span.Slice(1);
        }

        return result.ToArray();
    }

}
