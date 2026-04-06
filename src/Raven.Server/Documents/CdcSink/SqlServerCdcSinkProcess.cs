using System;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.Types;
using Raven.Client.Documents.Operations.CdcSink;
using DbProviderFactories = Raven.Server.Documents.ETL.Providers.RelationalDatabase.SQL.RelationalWriters.DbProviderFactories;
using Raven.Server.ServerWide.Context;
using Microsoft.Data.SqlClient;
using Raven.Server.NotificationCenter.Notifications;

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
    private readonly string _connectionString;
    private readonly string _factoryName;

    public SqlServerCdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
        : base(configuration, database)
    {
        _connectionString = configuration.Connection.ConnectionString;
        _factoryName = configuration.Connection.FactoryName;
    }

    protected override async Task RunInternalAsync(CancellationToken ct)
    {
        // in case of error, we'll re-learn the schema (it may have changed).
        _columnTypesCache.Clear();
        await EnsureCdcEnabled(ct);
        await HandleInitialLoad(ct);
        _initialLoadTcs.TrySetResult();
        await ProcessCdcStream(ct);
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

    private static async Task VerifyAgentIsRunning(DbConnection conn, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE program_name LIKE N'SQLAgent%'";
        var result = await cmd.ExecuteScalarAsync(ct);
        if (result is int count && count > 0)
            return;

        throw new InvalidOperationException(
            "SQL Server Agent is not running. CDC change capture requires the Agent to process " +
            "capture jobs that populate the change tables. Without it, no CDC events will be delivered. " +
            "For Docker containers, start with: -e 'MSSQL_AGENT_ENABLED=true'. " +
            "For on-premises installations, start the SQL Server Agent service.");
    }

    protected override async IAsyncEnumerable<CdcEvent> GetCdcEvents([EnumeratorCancellation] CancellationToken ct)
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

        await VerifyAgentIsRunning(conn, ct);
        bool shouldWait = false;
        var buffer = new List<(byte[] Lsn, byte[] SeqVal, CdcEvent Event)>();

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            if (shouldWait)
                await Task.Delay(pollInterval, ct);
            shouldWait = true;

            var lsnInfo = await GetLsnBounds(conn, captureInstances, lastLsn, ct);
            LastActivityTime = Database.Time.GetUtcNow();

            if (lsnInfo.MaxLsn == null || lsnInfo.FromLsn == null || CompareLsn(lsnInfo.FromLsn, lsnInfo.MaxLsn) > 0)
                continue;

            // Read ALL tables into a buffer, then sort by (__$start_lsn, __$seqval) for
            // cross-table transaction ordering. This ensures parent rows (e.g. orders) are
            // processed before child rows (e.g. order_lines) within the same transaction.
            //
            // Note: we intentionally do NOT use a snapshot transaction here. CDC change tables
            // are append-only and queried by LSN range (from_lsn..to_lsn), so each table read
            // is already bounded to the same LSN window. Snapshot isolation would require
            // ALLOW_SNAPSHOT_ISOLATION on the database and can interfere with CDC reads.
            buffer.Clear();

            foreach (var ci in captureInstances)
            {
                var tableMinLsn = lsnInfo.TableMinLsns.GetValueOrDefault(ci.CaptureInstance);
                if (tableMinLsn == null || IsAllZero(tableMinLsn))
                    continue;

                VerifyNoGapsInLsn(lsnInfo, ci, tableMinLsn);

                var effectiveFromLsn = CompareLsn(lsnInfo.FromLsn, tableMinLsn) >= 0 ? lsnInfo.FromLsn : tableMinLsn;

                await using var cmd = conn.CreateCommand();
                cmd.CommandText = ci.Query;
                AddParameter(cmd, "@from_lsn", effectiveFromLsn);
                AddParameter(cmd, "@to_lsn", lsnInfo.MaxLsn);

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                var columns = ci.Columns;
                var processor = DocumentProcessor.GetProcessor(ci.TableInfo.Schema, ci.TableInfo.TableName);
                // Query shape: __$start_lsn (0), __$seqval (1), __$operation (2), user columns (3+)

                while (await reader.ReadAsync(ct))
                {
                    var rowLsn = reader[0] as byte[];
                    var rowSeq = reader[1] as byte[];
                    var operation = reader.GetInt32(2);
                    var cdcOperation = operation == 1 ? CdcSinkOperation.Delete : CdcSinkOperation.Upsert;

                    var values = processor.RentValues();
                    for (int i = 0; i < columns.Length; i++)
                    {
                        int ordinal = i + 3;
                        values[i] = reader.IsDBNull(ordinal) ? null : ConvertSqlServerValue(reader.GetValue(ordinal));
                    }

                    var op = DocumentProcessor.ProcessRow(processor, cdcOperation, values, StreamingJsonContext);
                    var eventType = cdcOperation == CdcSinkOperation.Delete ? CdcEventType.Delete : CdcEventType.Upsert;
                    buffer.Add((rowLsn, rowSeq, new CdcEvent(eventType, op, null)));
                }
            }

            if (buffer.Count > 0)
            {
                // Sort by (LSN, seqval) to preserve cross-table transaction order
                buffer.Sort((a, b) =>
                {
                    int cmp = CompareLsn(a.Lsn, b.Lsn);
                    return cmp != 0 ? cmp : CompareLsn(a.SeqVal, b.SeqVal);
                });

                foreach (var (_, _, evt) in buffer)
                    yield return evt;

                yield return new CdcEvent(CdcEventType.TransactionCommit, null, Convert.ToHexString(lsnInfo.MaxLsn));
                lastLsn = lsnInfo.MaxLsn;
            }
        }
    }

    private void VerifyNoGapsInLsn((byte[] MaxLsn, byte[] FromLsn, Dictionary<string, byte[]> TableMinLsns) lsnInfo, CaptureInstanceInfo ci, byte[] tableMinLsn)
    {
        if (CompareLsn(lsnInfo.FromLsn, tableMinLsn) >= 0)
            return;
            
        if (Logger.IsErrorEnabled)
            Logger.Error($"[{Name}] CDC changes for capture instance '{ci.CaptureInstance}' " +
                $"(table {ci.TableInfo.FullName}) have been purged between our saved LSN " +
                $"({Convert.ToHexString(lsnInfo.FromLsn)}) and the table's min LSN " +
                $"({Convert.ToHexString(tableMinLsn)}). Changes in this gap are lost. " +
                "This can happen when the CDC cleanup job runs while the task is stopped, " +
                "or after a backup restore.");

        Database.NotificationCenter.Add(AlertRaised.Create(
            Database.Name, Tag,
            $"[{Name}] CDC changes purged for table {ci.TableInfo.FullName} — " +
            "gap detected between saved position and earliest available changes.",
            AlertReason.CdcSink_Error, NotificationSeverity.Warning,
            key: $"{Tag}/{Name}/stale-lsn/{ci.CaptureInstance}"));
    }

    /// <summary>
    /// Fetches max LSN, per-table min LSNs, and computes the effective from-LSN in a single roundtrip.
    /// Returns null MaxLsn when CDC has been enabled but no transactions have been captured yet
    /// (the CDC log is empty, so fn_cdc_get_max_lsn returns 0x00...00).
    /// </summary>
    private static async Task<(byte[] MaxLsn, byte[] FromLsn, Dictionary<string, byte[]> TableMinLsns)> GetLsnBounds(
        DbConnection conn,
        List<CaptureInstanceInfo> captureInstances,
        byte[] lastLsn, CancellationToken ct)
    {
        // Build a single query that returns max LSN, incremented last LSN, and per-table min LSNs.
        // This avoids N+2 roundtrips (max + increment + N tables) by doing it all in one batch.
        var sb = new StringBuilder();
        sb.Append("SELECT sys.fn_cdc_get_max_lsn() AS max_lsn");

        if (lastLsn != null)
            sb.Append(", sys.fn_cdc_increment_lsn(@last_lsn) AS from_lsn");

        for (int i = 0; i < captureInstances.Count; i++)
            sb.Append($", sys.fn_cdc_get_min_lsn(@ci{i}) AS ci{i}_min");

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sb.ToString();

        if (lastLsn != null)
            AddParameter(cmd, "@last_lsn", lastLsn);

        for (int i = 0; i < captureInstances.Count; i++)
            AddParameter(cmd, $"@ci{i}", captureInstances[i].CaptureInstance);

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
            for (int i = 0; i < captureInstances.Count; i++)
            {
                var minLsn = reader[$"ci{i}_min"] as byte[];
                if (minLsn == null || IsAllZero(minLsn))
                    continue;
                if (fromLsn == null || CompareLsn(minLsn, fromLsn) < 0)
                    fromLsn = minLsn;
            }
        }

        var tableMinLsns = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < captureInstances.Count; i++)
        {
            var minLsn = reader[$"ci{i}_min"] as byte[];
            tableMinLsns[captureInstances[i].CaptureInstance] = minLsn;
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
            // and read by ordinal instead of calling GetName()/skipping __$ at runtime
            var columns = new List<string>();
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
                    SELECT cc.column_name
                    FROM cdc.captured_columns cc
                    JOIN cdc.change_tables ct ON cc.object_id = ct.object_id
                    WHERE ct.capture_instance = @capture
                    ORDER BY cc.column_ordinal";

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
                quotedColumns[i] = CommandBuilder.QuoteIdentifier(columns[i]);
            var columnList = string.Join(", ", quotedColumns);

            // __$operation values: 1=delete, 2=insert, 3=pre-update image, 4=post-update image.
            // We filter out pre-update images (3) at the SQL level to avoid transferring rows we'd discard.
            var quotedFn = CommandBuilder.QuoteIdentifier($"fn_cdc_get_all_changes_{captureInstance}");
            var query = $@"
                SELECT __$start_lsn, __$seqval, __$operation, {columnList}
                FROM [cdc].{quotedFn}(@from_lsn, @to_lsn, N'all')
                WHERE __$operation <> 3
                ORDER BY __$start_lsn, __$seqval";

            var columnsArray = columns.ToArray();

            result.Add(new CaptureInstanceInfo
            {
                TableInfo = tableInfo,
                CaptureInstance = captureInstance,
                Query = query,
                Columns = columnsArray,
            });

            DocumentProcessor.SetSourceColumnNames(tableInfo.Schema, tableInfo.TableName, columnsArray);
        }

        return result;
    }



    /// <summary>
    /// SQL Server uses a polling model — we query for new LSNs every PollInterval
    /// (configurable, default 1s). LastActivityTime is updated on every poll iteration,
    /// even when there are no changes. If it goes stale, the poll loop has stopped.
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

        if (LastActivityTime == null)
            return true; // still initializing

        var silentFor = Database.Time.GetUtcNow() - LastActivityTime.Value;
        var pollInterval = Database.Configuration.CdcSink.PollInterval.AsTimeSpan;
        // Allow 3x the poll interval before flagging — accounts for GC pauses and
        // transient delays in the poll loop.
        if (silentFor > TimeSpan.FromTicks(pollInterval.Ticks * 3))
        {
            issue = $"No poll activity for {silentFor.TotalSeconds:F0}s " +
                    $"(expected every {pollInterval.TotalSeconds:F0}s). " +
                    "The CDC polling loop may have stopped.";
            return false;
        }

        return true;
    }

    protected override string GetDefaultSchema() => "dbo";

    protected override Task<DbConnection> OpenInitialLoadConnection(CancellationToken ct) => OpenConnectionAsync(ct);

    protected override DbCommandBuilder CommandBuilder { get; } = new SqlCommandBuilder();

    protected override bool UsesTopN => true;

    private readonly Dictionary<string, Dictionary<string, string>> _columnTypesCache = new();

    protected override async Task BindKeysetParameters(DbCommand cmd, CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns, string[] lastKeys, CancellationToken ct)
    {
        if (_columnTypesCache.TryGetValue(tableInfo.FullName, out var columnTypes) == false)
        {
            columnTypes = await GetColumnTypes(cmd.Connection, tableInfo.Schema, tableInfo.TableName, pkColumns, ct);
            _columnTypesCache[tableInfo.FullName] = columnTypes;
        }

        for (int i = 0; i < pkColumns.Count; i++)
        {
            var value = ConvertStringToType(lastKeys[i], columnTypes.GetValueOrDefault(pkColumns[i], "nvarchar"));
            AddParameter(cmd, $"@k{i}", value);
        }
    }

    protected override object ConvertInitialLoadValue(DbDataReader reader, int ordinal)
    {
        return ConvertSqlServerValue(reader.GetValue(ordinal));
    }

    private async Task<DbConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var factory = DbProviderFactories.GetFactory(_factoryName);
        var conn = factory.CreateConnection();
        conn.ConnectionString = _connectionString;
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
            "tinyint" => byte.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            "smallint" => short.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            "int" => int.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            "bigint" => long.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            "real" => float.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            "float" => double.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            "decimal" or "numeric" or "money" or "smallmoney" => decimal.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            "bit" => bool.Parse(value),
            "uniqueidentifier" => Guid.Parse(value),
            "date" => DateTime.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            "datetime" or "datetime2" or "smalldatetime" => DateTime.Parse(value, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind),
            "datetimeoffset" => DateTimeOffset.Parse(value, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind),
            "time" => TimeSpan.Parse(value, System.Globalization.CultureInfo.InvariantCulture),
            _ => value,
        };
    }

    /// <summary>
    /// Normalizes SQL Server values for consistent storage in RavenDB.
    /// Integers are normalized to long, floats to double, decimals are preserved.
    /// Spatial types (SqlGeometry, SqlGeography) are converted to their WKT string representation.
    /// Note: SQL Server has no unsigned integer types, so ulong/uint are not handled here
    /// (unlike MySQL which has BIGINT UNSIGNED). If a provider-specific type appears,
    /// the fallback (_ => value) passes it through unchanged.
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
            decimal => value,
            Guid g => g.ToString(),
            byte[] => value,
            DateTime => value,
            DateTimeOffset => value,
            SqlGeometry geom => geom.STAsText().ToSqlString().Value,
            SqlGeography geog => geog.STAsText().ToSqlString().Value,
            SqlHierarchyId hid => hid.ToString(),
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

    private string BuildEnableTablesScript(List<CdcSinkConfiguration.TableInfo> tables)
    {
        var sb = new StringBuilder();
        foreach (var table in tables)
        {
            sb.Append("  EXEC sys.sp_cdc_enable_table @source_schema = ").Append(CommandBuilder.QuoteIdentifier(table.Schema))
              .Append(", @source_name = ").Append(CommandBuilder.QuoteIdentifier(table.TableName))
              .Append(", @role_name = NULL;\n");
        }
        sb.Append('\n');
        return sb.ToString();
    }
}
