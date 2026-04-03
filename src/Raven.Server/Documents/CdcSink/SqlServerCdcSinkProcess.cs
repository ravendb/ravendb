using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
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
/// CDC Sink process that pulls change data from SQL Server using its native Change Data Capture feature.
///
/// <para><b>Startup:</b> Enables CDC on the database (<c>sp_cdc_enable_db</c>) and on each configured table
/// (<c>sp_cdc_enable_table</c>) if not already enabled. If the connection lacks permissions, the error
/// includes the exact admin script to run.</para>
///
/// <para><b>Initial Load:</b> Before streaming changes, performs a full table scan of each configured table
/// using keyset pagination (ordered by primary key). Rows are read in batches, processed through
/// <see cref="CdcSinkDocumentProcessor"/>, and written to RavenDB. Progress (last PK values per table)
/// is persisted so a restart resumes from where it left off rather than re-reading the entire table.</para>
///
/// <para><b>Change Polling:</b> After the initial load completes, enters a polling loop. Each iteration
/// opens a read transaction on the SQL Server connection for snapshot consistency, then executes a single
/// query to fetch the current max LSN, the incremented last-processed LSN, and per-table min LSNs.
/// Changes are read from <c>cdc.fn_cdc_get_all_changes_&lt;capture&gt;</c> for each table, filtered to
/// exclude pre-update images (operation 3), and ordered by <c>__$start_lsn, __$seqval</c> to preserve
/// transaction boundaries. The max LSN is saved as the checkpoint after each successful batch.</para>
///
/// <para><b>Consistency guarantee:</b> SQL Server CDC uses a pull model — change tables are populated
/// asynchronously by the capture job and cleaned up by a separate cleanup job. The polling transaction
/// ensures all LSN reads and change queries see the same point in time. If the cleanup job purges entries
/// before we read them, the per-table min LSN clamping ensures we start from the earliest available
/// row rather than requesting a purged range (which would error).</para>
/// </summary>
public class SqlServerCdcSinkProcess : CdcSinkProcess
{
    private readonly CdcSinkDocumentProcessor _documentProcessor;
    private readonly string _connectionString;
    private readonly string _factoryName;

    public SqlServerCdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
        : base(configuration, database)
    {
        _documentProcessor = new CdcSinkDocumentProcessor(configuration) { Logger = Logger };
        _connectionString = configuration.Connection.ConnectionString;
        _factoryName = configuration.Connection.FactoryName;
    }

    protected override async Task RunInternalAsync(CancellationToken ct)
    {
        await EnsureCdcEnabled(ct);
        await HandleInitialLoad(ct);
        _initialLoadTcs.TrySetResult();
        await PollForChanges(ct);
    }

    /// <summary>
    /// Enables CDC on the database and individual tables if not already enabled.
    /// Agent and job health checks are handled by <see cref="CdcSinkSourceVerifier"/>.
    /// </summary>
    private async Task EnsureCdcEnabled(CancellationToken ct)
    {
        await using var conn = await OpenConnectionAsync(ct);

        // Check and enable CDC on the database
        bool isCdcEnabled;
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT is_cdc_enabled FROM sys.databases WHERE name = DB_NAME()";
            var result = await cmd.ExecuteScalarAsync(ct);
            isCdcEnabled = result != null && result != DBNull.Value && Convert.ToInt32(result) == 1;
        }

        var allTables = Configuration.CollectAllTablesFlat("dbo");

        if (isCdcEnabled == false)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] Enabling CDC on the source database.");

            try
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "EXEC sys.sp_cdc_enable_db";
                await cmd.ExecuteNonQueryAsync(ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new InvalidOperationException(
                    $"""
                    Insufficient permissions to enable CDC on the database. 
                    SQL Server error: {ex.Message}

                    An administrator can enable it manually by running the following script:

                    EXEC sys.sp_cdc_enable_db;
                    {BuildEnableTablesScript(allTables)}
                    """, ex);
            }
        }

        // Enable CDC on each configured table
        var untrackedTables = new List<CdcSinkConfiguration.TableInfo>();
        foreach (var tableInfo in allTables)
        {
            bool isTracked;
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
                    SELECT t.is_tracked_by_cdc
                    FROM sys.tables t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE t.name = @tableName AND s.name = @schemaName";

                AddParameter(cmd, "@tableName", tableInfo.TableName);
                AddParameter(cmd, "@schemaName", tableInfo.Schema);

                var result = await cmd.ExecuteScalarAsync(ct);
                isTracked = result != null && result != DBNull.Value && Convert.ToInt32(result) == 1;
            }

            if (isTracked == false)
                untrackedTables.Add(tableInfo);
        }

        foreach (var tableInfo in untrackedTables)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] Enabling CDC tracking on table {tableInfo.FullName}.");

            try
            {
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "EXEC sys.sp_cdc_enable_table @source_schema = @schema, @source_name = @table, @role_name = NULL";
                AddParameter(cmd, "@schema", tableInfo.Schema);
                AddParameter(cmd, "@table", tableInfo.TableName);
                await cmd.ExecuteNonQueryAsync(ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new InvalidOperationException(
                    $"""
                    Insufficient permissions to enable CDC tracking on table '{tableInfo.FullName}'. 
                    SQL Server error: {ex.Message}
                    
                    An administrator can enable CDC for all required tables by running the following script:

                    {BuildEnableTablesScript(untrackedTables)}
                    """, ex);
            }
        }
    }

    private async Task PollForChanges(CancellationToken ct)
    {
        byte[] lastLsn;
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var state = LoadState(context);
            lastLsn = string.IsNullOrEmpty(state.LastLsn)
                ? null
                : Convert.FromHexString(state.LastLsn);
        }

        var captureInstances = await ResolveCaptureInstances(ct);
        var pollInterval = Database.Configuration.CdcSink.PollInterval.AsTimeSpan;

        await using var conn = await OpenConnectionAsync(ct);
        using var ___ = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext jsonParsingContext);
        bool shouldWait = false;

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            if (shouldWait)
                await Task.Delay(pollInterval, ct);
            shouldWait = true;

            // Use a transaction per iteration to get a consistent snapshot of the CDC state.
            // All LSN reads and change queries within a single iteration see the same point in time.
            // The transaction is read-only, so disposal (implicit rollback) is fine — no need to commit.
            await using var tx = await conn.BeginTransactionAsync(ct);
                var lsnInfo = await GetLsnBounds(conn, tx, captureInstances, lastLsn, ct);

                if (lsnInfo.MaxLsn == null || lsnInfo.FromLsn == null || CompareLsn(lsnInfo.FromLsn, lsnInfo.MaxLsn) > 0)
                    continue;

                var batch = new List<CdcSinkDocumentOp>();
                bool hasChanges = false;

                foreach (var ci in captureInstances)
                {
                    var tableMinLsn = lsnInfo.TableMinLsns.GetValueOrDefault(ci.CaptureInstance);
                    if (tableMinLsn == null || IsAllZero(tableMinLsn))
                        continue;

                    // fn_cdc_get_all_changes requires fromLsn >= fn_cdc_get_min_lsn(), otherwise it errors.
                    // This can happen when the CDC cleanup job purges old entries and our saved position
                    // points to an LSN that no longer exists in the change table.
                    var effectiveFromLsn = CompareLsn(lsnInfo.FromLsn, tableMinLsn) >= 0 ? lsnInfo.FromLsn : tableMinLsn;

                    await using var cmd = conn.CreateCommand();
                    cmd.Transaction = tx;
                    cmd.CommandText = ci.Query;
                    AddParameter(cmd, "@from_lsn", effectiveFromLsn);
                    AddParameter(cmd, "@to_lsn", lsnInfo.MaxLsn);

                    await using var reader = await cmd.ExecuteReaderAsync(ct);
                    var columns = ci.Columns;
                    while (await reader.ReadAsync(ct))
                    {
                        // __$operation is at ordinal 0 in our query (we select it first)
                        var operation = reader.GetInt32(0);

                        var cdcOperation = operation == 1 ? CdcSinkOperation.Delete : CdcSinkOperation.Upsert;

                        // Columns start at ordinal 1 (after __$operation), in the same order as ci.Columns
                        var data = new Dictionary<string, object>(columns.Length);
                        for (int i = 0; i < columns.Length; i++)
                        {
                            int ordinal = i + 1;
                            data[columns[i]] = reader.IsDBNull(ordinal) ? null : ConvertSqlServerValue(reader.GetValue(ordinal));
                        }

                        var row = new CdcSinkRow
                        {
                            TableSchema = ci.TableInfo.Schema,
                            TableName = ci.TableInfo.TableName,
                            Operation = cdcOperation,
                            Data = data,
                        };

                        var op = _documentProcessor.ProcessRow(row, jsonParsingContext);
                        if (op != null)
                        {
                            batch.Add(op);
                            hasChanges = true;
                        }
                    }
                }

                if (hasChanges)
                {
                    var lsnHex = Convert.ToHexString(lsnInfo.MaxLsn);
                    await SubmitBatch(batch, lsnHex);
                    lastLsn = lsnInfo.MaxLsn;
                }
        }
    }

    /// <summary>
    /// Fetches max LSN, per-table min LSNs, and computes the effective from-LSN in a single roundtrip.
    /// Returns null MaxLsn when CDC has been enabled but no transactions have been captured yet
    /// (the CDC log is empty, so fn_cdc_get_max_lsn returns 0x00...00).
    /// </summary>
    private static async Task<(byte[] MaxLsn, byte[] FromLsn, Dictionary<string, byte[]> TableMinLsns)> GetLsnBounds(
        DbConnection conn, DbTransaction tx,
        List<CaptureInstanceInfo> captureInstances,
        byte[] lastLsn, CancellationToken ct)
    {
        // Build a single query that returns max LSN, incremented last LSN, and per-table min LSNs.
        // This avoids N+2 roundtrips (max + increment + N tables) by doing it all in one batch.
        var sb = new StringBuilder();
        sb.Append("SELECT sys.fn_cdc_get_max_lsn() AS max_lsn");

        if (lastLsn != null)
            sb.Append(", sys.fn_cdc_increment_lsn(@last_lsn) AS from_lsn");

        foreach (var ci in captureInstances)
            sb.Append($", sys.fn_cdc_get_min_lsn('{ci.CaptureInstance}') AS [{ci.CaptureInstance}_min]");

        await using var cmd = conn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = sb.ToString();

        if (lastLsn != null)
            AddParameter(cmd, "@last_lsn", lastLsn);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct) == false)
            return (null, null, null);

        var maxLsn = reader["max_lsn"] as byte[];
        if (maxLsn == null || IsAllZero(maxLsn))
            return (null, null, null);

        byte[] fromLsn;
        if (lastLsn != null)
        {
            fromLsn = reader["from_lsn"] as byte[];
        }
        else
        {
            // No previous LSN — find the global minimum across all capture instances
            fromLsn = null;
            foreach (var ci in captureInstances)
            {
                var minLsn = reader[$"{ci.CaptureInstance}_min"] as byte[];
                if (minLsn == null || IsAllZero(minLsn))
                    continue;
                if (fromLsn == null || CompareLsn(minLsn, fromLsn) < 0)
                    fromLsn = minLsn;
            }
        }

        var tableMinLsns = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
        foreach (var ci in captureInstances)
        {
            var minLsn = reader[$"{ci.CaptureInstance}_min"] as byte[];
            tableMinLsns[ci.CaptureInstance] = minLsn;
        }

        return (maxLsn, fromLsn, tableMinLsns);
    }

    private sealed class CaptureInstanceInfo
    {
        public CdcSinkConfiguration.TableInfo TableInfo;
        public string CaptureInstance;
        /// <summary>Pre-built SELECT query with explicit column list and ORDER BY.</summary>
        public string Query;
        /// <summary>Column names in the same order as the SELECT list (excludes CDC metadata columns).</summary>
        public string[] Columns;
    }

    private async Task<List<CaptureInstanceInfo>> ResolveCaptureInstances(CancellationToken ct)
    {
        var result = new List<CaptureInstanceInfo>();
        var allTables = Configuration.CollectAllTablesFlat("dbo");

        await using var conn = await OpenConnectionAsync(ct);

        foreach (var tableInfo in allTables)
        {
            string captureInstance = null;

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
                    SELECT capture_instance
                    FROM cdc.change_tables
                    WHERE source_object_id = OBJECT_ID(@fullTableName)";

                AddParameter(cmd, "@fullTableName", $"{tableInfo.Schema}.{tableInfo.TableName}");
                var val = await cmd.ExecuteScalarAsync(ct);
                captureInstance = val?.ToString();
            }

            if (string.IsNullOrEmpty(captureInstance))
            {
                // Default capture instance naming convention
                captureInstance = $"{tableInfo.Schema}_{tableInfo.TableName}";
            }

            // Fetch the captured column names so we can build an explicit SELECT
            // and read by ordinal instead of calling GetName()/skipping __$ at runtime.
            var columns = new List<string>();
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
                    SELECT column_name
                    FROM cdc.captured_columns
                    WHERE capture_instance = @capture
                    ORDER BY column_ordinal";

                AddParameter(cmd, "@capture", captureInstance);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                    columns.Add(reader.GetString(0));
            }

            if (columns.Count == 0)
                throw new InvalidOperationException(
                    $"No captured columns found for CDC capture instance '{captureInstance}' (table {tableInfo.FullName}). " +
                    "Verify that CDC tracking is enabled on this table.");

            var quotedColumns = new string[columns.Count];
            for (int i = 0; i < columns.Count; i++)
                quotedColumns[i] = $"[{columns[i]}]";
            var columnList = string.Join(", ", quotedColumns);
            // __$operation values: 1=delete, 2=insert, 3=pre-update image, 4=post-update image.
            // We filter out pre-update images (3) at the SQL level to avoid transferring rows we'd discard.
            var query = $@"
                SELECT __$operation, {columnList}
                FROM cdc.fn_cdc_get_all_changes_{captureInstance}(@from_lsn, @to_lsn, N'all')
                WHERE __$operation <> 3
                ORDER BY __$start_lsn, __$seqval";

            result.Add(new CaptureInstanceInfo
            {
                TableInfo = tableInfo,
                CaptureInstance = captureInstance,
                Query = query,
                Columns = columns.ToArray(),
            });
        }

        return result;
    }



    private async Task HandleInitialLoad(CancellationToken ct)
    {
        CdcSinkTaskState state;
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            state = LoadState(context);
        }

        var allTables = Configuration.CollectAllTablesFlat("dbo");

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

    /// <summary>
    /// Reads one table in batches using TOP N with keyset pagination,
    /// overlapping reads with TxMerger writes (same pattern as PostgresCdcSinkProcess).
    /// </summary>
    private async Task ProcessTableInitialLoad(
        CdcSinkConfiguration.TableInfo tableInfo, string tableKey,
        CdcSinkTableLoadState resumeState, CancellationToken ct)
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
        await using var conn = await OpenConnectionAsync(ct);

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
                await SubmitBatch(new List<CdcSinkDocumentOp>(), tableLoadUpdates: finalUpdate);
                break;
            }

            await lastBatch;

            var tableLoadUpdate = new Dictionary<string, CdcSinkTableLoadState>
            {
                [tableKey] = new CdcSinkTableLoadState { LastKeyValues = new List<string>(newLastKeys) }
            };

            lastBatch = SubmitBatch(ops, tableLoadUpdates: tableLoadUpdate);
            lastKeys = newLastKeys;
        }

        await lastBatch;
    }

    private async Task<(List<CdcSinkDocumentOp> Ops, string[] LastKeys)> ReadOneBatch(
        DbConnection conn, CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns,
        string[] lastKeys, int maxBatchSize, Dictionary<string, string> columnTypes, CancellationToken ct)
    {
        // Build ORDER BY clause with bracket-quoted column names
        var orderByParts = new string[pkColumns.Count];
        for (int i = 0; i < pkColumns.Count; i++)
            orderByParts[i] = $"[{pkColumns[i]}]";
        var orderBy = string.Join(", ", orderByParts);

        DbCommand cmd;
        if (lastKeys != null)
        {
            // Keyset pagination with row-value comparison: WHERE (col1, col2) > (@k0, @k1)
            var paramPlaceholders = new string[pkColumns.Count];
            for (int i = 0; i < pkColumns.Count; i++)
                paramPlaceholders[i] = $"@k{i}";

            var columnList = string.Join(", ", orderByParts);
            var paramList = string.Join(", ", paramPlaceholders);

            var query = $"SELECT TOP ({maxBatchSize}) * FROM [{tableInfo.Schema}].[{tableInfo.TableName}] " +
                        $"WHERE ({columnList}) > ({paramList}) ORDER BY {orderBy}";

            cmd = conn.CreateCommand();
            cmd.CommandText = query;

            for (int i = 0; i < pkColumns.Count; i++)
            {
                var value = ConvertStringToType(lastKeys[i], columnTypes.GetValueOrDefault(pkColumns[i], "nvarchar"));
                AddParameter(cmd, $"@k{i}", value);
            }
        }
        else
        {
            var query = $"SELECT TOP ({maxBatchSize}) * FROM [{tableInfo.Schema}].[{tableInfo.TableName}] ORDER BY {orderBy}";
            cmd = conn.CreateCommand();
            cmd.CommandText = query;
        }

        await using (cmd)
        {
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            using var ____ = Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext jsonParsingCtx);

            var ops = new List<CdcSinkDocumentOp>();

            while (await reader.ReadAsync(ct))
            {
                var data = new Dictionary<string, object>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var name = reader.GetName(i);
                    var value = reader.IsDBNull(i) ? null : ConvertSqlServerValue(reader.GetValue(i));
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

    private async Task<DbConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var factory = DbProviderFactories.GetFactory(_factoryName);
        var conn = factory.CreateConnection();
        conn.ConnectionString = _connectionString;
        await conn.OpenAsync(ct);
        return conn;
    }



    private static async Task<Dictionary<string, string>> GetColumnTypes(
        DbConnection conn, string schema, string tableName, List<string> columns, CancellationToken ct)
    {
        var types = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table";

        AddParameter(cmd, "@schema", schema);
        AddParameter(cmd, "@table", tableName);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var colName = reader.GetString(0);
            for (int i = 0; i < columns.Count; i++)
            {
                if (string.Equals(colName, columns[i], StringComparison.OrdinalIgnoreCase))
                {
                    types[colName] = reader.GetString(1).ToLowerInvariant();
                    break;
                }
            }
        }

        return types;
    }

    private static object ConvertStringToType(string value, string normalizedType)
    {
        if (string.IsNullOrEmpty(value))
            return DBNull.Value;

        return normalizedType switch
        {
            "tinyint" => byte.Parse(value),
            "smallint" => short.Parse(value),
            "int" => int.Parse(value),
            "bigint" => long.Parse(value),
            "real" => float.Parse(value),
            "float" => double.Parse(value),
            "decimal" or "numeric" or "money" or "smallmoney" => decimal.Parse(value),
            "bit" => bool.Parse(value),
            "uniqueidentifier" => Guid.Parse(value),
            _ => value,
        };
    }

    /// <summary>
    /// Normalizes SQL Server values for consistent storage in RavenDB.
    /// Integers are normalized to long, floats to double.
    /// </summary>
    private static object ConvertSqlServerValue(object value)
    {
        if (value is null || value == DBNull.Value)
            return null;

        return value switch
        {
            byte b => (long)b,
            short s => (long)s,
            int i => (long)i,
            long => value,
            float f => (double)f,
            double => value,
            decimal d => (double)d,
            Guid g => g.ToString(),
            byte[] => value,
            DateTime => value,
            DateTimeOffset => value,
            _ => value,
        };
    }


    private static int CompareLsn(byte[] a, byte[] b)
    {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        return ((ReadOnlySpan<byte>)a).SequenceCompareTo(b);
    }

    private static bool IsAllZero(byte[] bytes)
    {
        return MemoryExtensions.ContainsAnyExcept((ReadOnlySpan<byte>)bytes, (byte)0) == false;
    }

    private static string BuildEnableTablesScript(List<CdcSinkConfiguration.TableInfo> tables)
    {
        var sb = new StringBuilder();
        foreach (var table in tables)
        {
            sb.Append("  EXEC sys.sp_cdc_enable_table @source_schema = N'").Append(table.Schema)
              .Append("', @source_name = N'").Append(table.TableName)
              .Append("', @role_name = NULL;\n");
        }
        sb.Append('\n');
        return sb.ToString();
    }
}
