using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
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

public class SqlServerCdcSinkProcess : CdcSinkProcess
{
    private readonly CdcSinkDocumentProcessor _documentProcessor;
    private readonly string _connectionString;
    private readonly string _factoryName;

    private static readonly TimeSpan DefaultPollInterval = TimeSpan.FromSeconds(1);

    public SqlServerCdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
        : base(configuration, database)
    {
        _documentProcessor = new CdcSinkDocumentProcessor(configuration);
        _connectionString = configuration.Connection.ConnectionString;
        _factoryName = configuration.Connection.FactoryName;
    }

    protected override ICdcSinkConsumer CreateConsumer()
    {
        throw new NotSupportedException("SqlServerCdcSinkProcess uses its own Run() loop instead of ICdcSinkConsumer.");
    }

    protected override void Run()
    {
        AsyncHelpers.RunSync(() => RunAsync(CancellationToken));
    }

    private async Task RunAsync(CancellationToken ct)
    {
        try
        {
            await EnsureCdcEnabled(ct);
            await HandleInitialLoad(ct);
            await PollForChanges(ct);
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

        if (isCdcEnabled == false)
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"[{Name}] Enabling CDC on the source database.");

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "EXEC sys.sp_cdc_enable_db";
            await cmd.ExecuteNonQueryAsync(ct);
        }

        // Enable CDC on each configured table
        var allTables = CollectAllTablesFlat();
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
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"[{Name}] Enabling CDC tracking on table {tableInfo.FullName}.");

                await using var cmd = conn.CreateCommand();
                cmd.CommandText = $"EXEC sys.sp_cdc_enable_table @source_schema = '{tableInfo.Schema}', @source_name = '{tableInfo.TableName}', @role_name = NULL";
                await cmd.ExecuteNonQueryAsync(ct);
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
                : HexStringToBytes(state.LastLsn);
        }

        // Build capture instance mapping for each table
        var captureInstances = await ResolveCaptureInstances(ct);

        while (true)
        {
            ct.ThrowIfCancellationRequested();

            await using var conn = await OpenConnectionAsync(ct);

            // Get the current max LSN
            byte[] maxLsn;
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT sys.fn_cdc_get_max_lsn()";
                var result = await cmd.ExecuteScalarAsync(ct);
                maxLsn = result as byte[];
            }

            if (maxLsn == null || maxLsn.All(b => b == 0))
            {
                await Task.Delay(DefaultPollInterval, ct);
                continue;
            }

            // If we have no previous LSN, start from the minimum available
            byte[] fromLsn;
            if (lastLsn == null)
            {
                fromLsn = await GetGlobalMinLsn(conn, captureInstances, ct);
                if (fromLsn == null)
                {
                    await Task.Delay(DefaultPollInterval, ct);
                    continue;
                }
            }
            else
            {
                // Increment the last LSN to avoid re-reading the same changes
                fromLsn = await IncrementLsn(conn, lastLsn, ct);
                if (fromLsn == null)
                {
                    await Task.Delay(DefaultPollInterval, ct);
                    continue;
                }
            }

            if (CompareLsn(fromLsn, maxLsn) > 0)
            {
                await Task.Delay(DefaultPollInterval, ct);
                continue;
            }

            var batch = new List<CdcSinkDocumentOp>();
            bool hasChanges = false;

            foreach (var (tableInfo, captureInstance) in captureInstances)
            {
                // Get the min LSN for this capture instance to avoid querying before it's available
                byte[] tableMinLsn;
                await using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"SELECT sys.fn_cdc_get_min_lsn('{captureInstance}')";
                    var result = await cmd.ExecuteScalarAsync(ct);
                    tableMinLsn = result as byte[];
                }

                if (tableMinLsn == null || tableMinLsn.All(b => b == 0))
                    continue;

                // Use the greater of our fromLsn and the table's min LSN
                var effectiveFromLsn = CompareLsn(fromLsn, tableMinLsn) >= 0 ? fromLsn : tableMinLsn;

                if (CompareLsn(effectiveFromLsn, maxLsn) > 0)
                    continue;

                await using var cmd2 = conn.CreateCommand();
                cmd2.CommandText = $"SELECT * FROM cdc.fn_cdc_get_all_changes_{captureInstance}(@from_lsn, @to_lsn, N'all update old')";
                AddParameter(cmd2, "@from_lsn", effectiveFromLsn);
                AddParameter(cmd2, "@to_lsn", maxLsn);

                await using var reader = await cmd2.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var operation = reader.GetInt32(reader.GetOrdinal("__$operation"));

                    // Skip pre-update images (operation = 3)
                    if (operation == 3)
                        continue;

                    var cdcOperation = operation == 1 ? CdcSinkOperation.Delete : CdcSinkOperation.Upsert;

                    var data = new Dictionary<string, object>();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var colName = reader.GetName(i);
                        // Skip CDC metadata columns
                        if (colName.StartsWith("__$"))
                            continue;

                        data[colName] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    }

                    var row = new CdcSinkRow
                    {
                        TableSchema = tableInfo.Schema,
                        TableName = tableInfo.TableName,
                        Operation = cdcOperation,
                        Data = data,
                    };

                    var op = _documentProcessor.ProcessRow(row);
                    batch.Add(op);
                    hasChanges = true;
                }
            }

            if (hasChanges)
            {
                var lsnHex = BytesToHexString(maxLsn);
                await SubmitBatch(batch, lsnHex);
                lastLsn = maxLsn;
            }

            await Task.Delay(DefaultPollInterval, ct);
        }
    }

    private async Task<Dictionary<TableInfo, string>> ResolveCaptureInstances(CancellationToken ct)
    {
        var result = new Dictionary<TableInfo, string>();
        var allTables = CollectAllTablesFlat();

        await using var conn = await OpenConnectionAsync(ct);

        foreach (var tableInfo in allTables)
        {
            string captureInstance = null;

            // Try to resolve the capture instance from the CDC metadata
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

            result[tableInfo] = captureInstance;
        }

        return result;
    }

    private async Task<byte[]> GetGlobalMinLsn(DbConnection conn, Dictionary<TableInfo, string> captureInstances, CancellationToken ct)
    {
        byte[] globalMin = null;

        foreach (var (_, captureInstance) in captureInstances)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT sys.fn_cdc_get_min_lsn('{captureInstance}')";
            var result = await cmd.ExecuteScalarAsync(ct);
            var minLsn = result as byte[];

            if (minLsn == null || minLsn.All(b => b == 0))
                continue;

            if (globalMin == null || CompareLsn(minLsn, globalMin) < 0)
                globalMin = minLsn;
        }

        return globalMin;
    }

    private async Task<byte[]> IncrementLsn(DbConnection conn, byte[] lsn, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT sys.fn_cdc_increment_lsn(@lsn)";
        AddParameter(cmd, "@lsn", lsn);
        var result = await cmd.ExecuteScalarAsync(ct);
        return result as byte[];
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
        await using var conn = await OpenConnectionAsync(ct);

        var pkColumns = tableInfo.PrimaryKeyColumns;
        var orderBy = string.Join(", ", pkColumns);
        var query = $"SELECT * FROM [{tableInfo.Schema}].[{tableInfo.TableName}] ORDER BY {orderBy}";

        if (resumeState?.LastKeyValues != null && resumeState.LastKeyValues.Count == pkColumns.Count)
        {
            var columnTypes = await GetColumnTypes(conn, tableInfo.Schema, tableInfo.TableName, pkColumns, ct);
            var whereParts = $"({string.Join(", ", pkColumns)}) > ({string.Join(", ", pkColumns.Select((_, i) => $"@k{i}"))})";
            query = $"SELECT * FROM [{tableInfo.Schema}].[{tableInfo.TableName}] WHERE {whereParts} ORDER BY {orderBy}";

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = query;
            for (int i = 0; i < pkColumns.Count; i++)
            {
                var value = ConvertStringToType(resumeState.LastKeyValues[i], columnTypes.GetValueOrDefault(pkColumns[i], "nvarchar"));
                AddParameter(cmd, $"@k{i}", value);
            }

            await ProcessInitialLoadReader(cmd, tableInfo, tableKey, pkColumns, ct);
        }
        else
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = query;
            await ProcessInitialLoadReader(cmd, tableInfo, tableKey, pkColumns, ct);
        }
    }

    private async Task ProcessInitialLoadReader(
        DbCommand cmd, TableInfo tableInfo, string tableKey,
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
            var command = new CdcSinkBatchCommand(
                Database, new List<CdcSinkDocumentOp>(), Configuration.Name, lastLsn: null,
                tableLoadUpdates: finalUpdate,
                statsScope: null, statistics: Statistics, logger: Logger);

            Database.TxMerger.EnqueueSync(command);
        }
    }

    private Task SubmitBatch(List<CdcSinkDocumentOp> ops, string lastLsn)
    {
        var command = new CdcSinkBatchCommand(
            Database, ops, Configuration.Name, lastLsn,
            tableLoadUpdates: null,
            statsScope: null, statistics: Statistics, logger: Logger);

        return Database.TxMerger.Enqueue(command);
    }

    private CdcSinkTaskState LoadState(DocumentsOperationContext context)
    {
        var stateDocId = CdcSinkTaskState.GetDocumentId(Configuration.Name);
        var doc = Database.DocumentsStorage.Get(context, stateDocId);

        if (doc == null)
            return new CdcSinkTaskState { ConfigurationName = Configuration.Name };

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
            var schema = table.SourceTableSchema ?? "dbo";
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
            var schema = e.SourceTableSchema ?? "dbo";
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
                Schema = table.SourceTableSchema ?? "dbo",
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
                Schema = e.SourceTableSchema ?? "dbo",
                TableName = e.SourceTableName,
                PrimaryKeyColumns = e.PrimaryKeyColumns,
            });
            CollectEmbeddedTablesFlat(e.EmbeddedTables, tables);
        }
    }

    private async Task<DbConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var factory = DbProviderFactories.GetFactory(_factoryName);
        var conn = factory.CreateConnection();
        conn.ConnectionString = _connectionString;
        await conn.OpenAsync(ct);
        return conn;
    }

    private static void AddParameter(DbCommand cmd, string name, object value)
    {
        var param = cmd.CreateParameter();
        param.ParameterName = name;
        param.Value = value ?? DBNull.Value;
        cmd.Parameters.Add(param);
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
            if (columns.Contains(colName, StringComparer.OrdinalIgnoreCase))
                types[colName] = reader.GetString(1);
        }

        return types;
    }

    private static object ConvertStringToType(string value, string sqlType)
    {
        if (string.IsNullOrEmpty(value))
            return DBNull.Value;

        return sqlType.ToLower() switch
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

    private static int CompareLsn(byte[] a, byte[] b)
    {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;

        for (int i = 0; i < Math.Min(a.Length, b.Length); i++)
        {
            if (a[i] < b[i]) return -1;
            if (a[i] > b[i]) return 1;
        }

        return a.Length.CompareTo(b.Length);
    }

    private static string BytesToHexString(byte[] bytes)
    {
        return Convert.ToHexString(bytes);
    }

    private static byte[] HexStringToBytes(string hex)
    {
        return Convert.FromHexString(hex);
    }

    internal class TableInfo
    {
        public string Schema { get; init; }
        public string TableName { get; init; }
        public List<string> PrimaryKeyColumns { get; init; }
        public string FullName => $"{Schema}.{TableName}";

        public override int GetHashCode() => StringComparer.OrdinalIgnoreCase.GetHashCode(FullName);
        public override bool Equals(object obj) => obj is TableInfo other && string.Equals(FullName, other.FullName, StringComparison.OrdinalIgnoreCase);
    }
}
