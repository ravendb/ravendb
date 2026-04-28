using System;
using System.Runtime.CompilerServices;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using MySqlCdc;
using MySqlCdc.Constants;
using MySqlCdc.Events;
using MySqlCdc.Providers.MariaDb;
using MySqlCdc.Providers.MySql;
using MySqlConnector;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.CdcSink;

/// <summary>
/// CDC Sink process for MySQL and MariaDB using binlog replication.
///
/// <para><b>Architecture:</b> Uses the MySqlCdc library to connect as a MySQL replica and
/// stream row-based binlog events in real-time. Initial load uses MySqlConnector for
/// regular SQL queries with keyset pagination (same pattern as PostgreSQL and SQL Server).</para>
///
/// <para><b>Initial Load:</b> Full table scan via <c>SELECT * FROM table ORDER BY pk LIMIT N</c>
/// with keyset pagination for resume support. Overlaps reads with TxMerger writes.</para>
///
/// <para><b>Binlog Streaming:</b> After the initial load, connects as a binlog replica using
/// GTID-based positioning. Events arrive as <c>WriteRowsEvent</c>, <c>UpdateRowsEvent</c>,
/// <c>DeleteRowsEvent</c> within transaction boundaries (<c>GtidEvent</c>...<c>XidEvent</c>).
/// Rows within a transaction are buffered and flushed on commit.</para>
///
/// <para><b>MySQL vs MariaDB:</b> Auto-detected via <c>SELECT VERSION()</c>. The correct
/// event deserializer and GTID format are chosen automatically. GTID is treated as an
/// opaque string — stored and restored without parsing.</para>
/// </summary>
public class MySqlCdcSinkProcess : CdcSinkProcess
{
    private readonly string _connectionString;
    private bool _isMariaDb;
    private string _serverGtid; // Current GTID set fetched from server during startup

    private enum MySqlColumnCategory { Other, Text, Decimal, Json, Boolean }

    private readonly record struct ColumnInfo(string Name, MySqlColumnCategory Category, string DataType);

    /// <summary>
    /// Cached table metadata resolved from INFORMATION_SCHEMA during startup.
    /// Binlog row events carry only positional values (no column names), so we
    /// need the column list + IsText flags to decode them. The processor is also
    /// cached here to avoid per-row dictionary lookups during streaming.
    /// </summary>
    private sealed class TableInfo
    {
        public required ColumnInfo[] Columns { get; init; }
        public required CdcSinkTableProcessor Processor { get; init; }

        /// <summary>
        /// Expected binlog type bytes for column positions 0..RequiredPrefixLength-1,
        /// computed from INFORMATION_SCHEMA DATA_TYPE at resolve time. Used for prefix
        /// comparison on each TableMapEvent to detect schema changes.
        /// </summary>
        public required byte[] ExpectedTypesPrefix { get; init; }

        /// <summary>
        /// The TableId from the last TableMapEvent that matched our expected schema.
        /// <see cref="TableIdReplayingOldEvents"/>: replaying from an old GTID, skip mismatches (old-schema events expected).
        /// <see cref="TableIdExpectCurrentSchema"/>: checkpoint is recent, throw on mismatch (schema changed since we started).
        /// Any other value: cached real TableId from the last matching TableMapEvent.
        /// </summary>
        public long ValidTableId { get; set; } = TableIdReplayingOldEvents;
    }

    /// <summary>No valid schema seen yet — replaying from old GTID, skip mismatched TableMapEvents.</summary>
    private const long TableIdReplayingOldEvents = -1;

    /// <summary>Checkpoint is at a recent position — any TableMapEvent mismatch is a real schema change, throw.</summary>
    private const long TableIdExpectCurrentSchema = -2;

    /// <summary>
    /// Maps MySQL INFORMATION_SCHEMA.DATA_TYPE strings to binlog ColumnType byte values.
    /// These are the type codes that appear in TableMapEvent.ColumnTypes.
    /// </summary>
    private static readonly Dictionary<string, byte> MySqlDataTypeToBinlogType = new(StringComparer.OrdinalIgnoreCase)
    {
        // Integer types
        ["tinyint"]    = 1,   // MYSQL_TYPE_TINY
        ["smallint"]   = 2,   // MYSQL_TYPE_SHORT
        ["mediumint"]  = 9,   // MYSQL_TYPE_INT24
        ["int"]        = 3,   // MYSQL_TYPE_LONG
        ["integer"]    = 3,   // alias for int
        ["bigint"]     = 8,   // MYSQL_TYPE_LONGLONG

        // Floating point
        ["float"]      = 4,   // MYSQL_TYPE_FLOAT
        ["double"]     = 5,   // MYSQL_TYPE_DOUBLE
        ["decimal"]    = 246, // MYSQL_TYPE_NEWDECIMAL
        ["numeric"]    = 246, // alias for decimal

        // String types
        ["char"]       = 254, // MYSQL_TYPE_STRING
        ["varchar"]    = 15,  // MYSQL_TYPE_VARCHAR
        ["tinytext"]   = 252, // MYSQL_TYPE_BLOB (text types share BLOB type code)
        ["text"]       = 252, // MYSQL_TYPE_BLOB
        ["mediumtext"] = 252, // MYSQL_TYPE_BLOB
        ["longtext"]   = 252, // MYSQL_TYPE_BLOB
        ["tinyblob"]   = 252, // MYSQL_TYPE_BLOB
        ["blob"]       = 252, // MYSQL_TYPE_BLOB
        ["mediumblob"] = 252, // MYSQL_TYPE_BLOB
        ["longblob"]   = 252, // MYSQL_TYPE_BLOB
        ["binary"]     = 254, // MYSQL_TYPE_STRING
        ["varbinary"]  = 15,  // MYSQL_TYPE_VARCHAR

        // Date/time types
        // MYSQL_TYPE_NEWDATE (14) is an internal storage optimization; binlog row
        // events emit the legacy MYSQL_TYPE_DATE (10) for `DATE` columns.
        ["date"]       = 10,  // MYSQL_TYPE_DATE
        ["time"]       = 19,  // MYSQL_TYPE_TIME2
        ["datetime"]   = 18,  // MYSQL_TYPE_DATETIME2
        ["timestamp"]  = 17,  // MYSQL_TYPE_TIMESTAMP2
        ["year"]       = 13,  // MYSQL_TYPE_YEAR

        // Other types
        ["bit"]        = 16,  // MYSQL_TYPE_BIT
        ["enum"]       = 254, // MYSQL_TYPE_STRING (encoded as string)
        ["set"]        = 254, // MYSQL_TYPE_STRING (encoded as string)
        ["json"]       = 245, // MYSQL_TYPE_JSON
        ["geometry"]   = 255, // MYSQL_TYPE_GEOMETRY
    };

    internal static byte MapDataTypeToBinlogType(string dataType)
    {
        if (MySqlDataTypeToBinlogType.TryGetValue(dataType, out var binlogType))
            return binlogType;

        // Unknown types are not fatal — we just can't do prefix comparison for this position.
        // Use 0 as a wildcard that won't match anything, forcing a restart if it matters.
        return 0;
    }

    private readonly Dictionary<(string Schema, string Table), TableInfo> _resolvedTables = new(TableKeyComparer.Instance);

    public MySqlCdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
        : base(configuration, database, GetDefaultSchemaFromConnectionString(configuration.Connection.ConnectionString))
    {
        _connectionString = configuration.Connection.ConnectionString;
    }

    private static string GetDefaultSchemaFromConnectionString(string connectionString)
    {
        var builder = new MySqlConnectionStringBuilder(connectionString);
        return builder.Database ?? "mysql";
    }

    protected override async Task RunInternalAsync(CancellationToken ct)
    {
        _resolvedTables.Clear();
        await EnsureBinlogConfiguration(ct);
        await ResolveColumnNames(ct);
        await HandleInitialLoad(ct);

        // After initial load, the checkpoint is at a recent GTID position.
        // Mark all tables as expecting the current schema (-2) so that
        // TableMapEvent mismatches throw immediately instead of being
        // silently skipped. 
        foreach (var table in _resolvedTables.Values)
            table.ValidTableId = TableIdExpectCurrentSchema;

        _initialLoadTcs.TrySetResult();
        await ProcessCdcStream(ct);
    }

    protected override Task<string> ReadCurrentCheckpointAsync(CancellationToken ct)
        => Task.FromResult(_serverGtid);

    /// <summary>
    /// Verifies that the MySQL/MariaDB server is correctly configured for CDC:
    /// binlog_format=ROW, binlog_row_image=FULL, GTID enabled, and correct permissions.
    /// Also detects whether this is MySQL or MariaDB for deserializer selection.
    /// </summary>
    private async Task EnsureBinlogConfiguration(CancellationToken ct)
    {
        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        // Detect MySQL vs MariaDB
        string version;
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT VERSION()";
            version = (string)await cmd.ExecuteScalarAsync(ct);
        }
        _isMariaDb = version != null && version.Contains("MariaDB", StringComparison.OrdinalIgnoreCase);

        // Check binlog_format
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT @@binlog_format";
            var format = (string)await cmd.ExecuteScalarAsync(ct);
            if (string.Equals(format, "ROW", StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new InvalidOperationException(
                    $"""
                    MySQL binlog_format is '{format}' but must be 'ROW' for CDC Sink.
                    {(_isMariaDb ? "MariaDB defaults to MIXED — " : "")}Set it in the server configuration:

                      SET GLOBAL binlog_format = 'ROW';

                    Or add to my.cnf / my.ini:
                      [mysqld]
                      binlog_format = ROW
                    """);
            }
        }

        // Check binlog_row_image
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT @@binlog_row_image";
            var rowImage = (string)await cmd.ExecuteScalarAsync(ct);
            if (string.Equals(rowImage, "FULL", StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new InvalidOperationException(
                    $"""
                    MySQL binlog_row_image is '{rowImage}' but must be 'FULL' for CDC Sink.
                    This ensures all column values are available in change events.

                      SET GLOBAL binlog_row_image = 'FULL';
                    """);
            }
        }

        // Verify GTID is enabled
        if (_isMariaDb == false)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT @@gtid_mode";
            var gtidMode = (string)await cmd.ExecuteScalarAsync(ct);
            if (string.Equals(gtidMode, "ON", StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new InvalidOperationException(
                    $"""
                    MySQL gtid_mode is '{gtidMode}' but must be 'ON' for CDC Sink.
                    GTID provides reliable position tracking for change data capture.

                      SET GLOBAL gtid_mode = ON;
                      SET GLOBAL enforce_gtid_consistency = ON;

                    Note: Enabling GTID requires all transactions to be GTID-safe.
                    """);
            }
        }
        // MariaDB always has GTID support enabled (no gtid_mode setting)

        // Fetch the current server GTID set so we can start streaming from this point after the initial load completes.
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = _isMariaDb
                ? "SELECT @@gtid_current_pos"
                : "SELECT @@gtid_executed";
            _serverGtid = (string)await cmd.ExecuteScalarAsync(ct);
        }

    }

    /// <summary>
    /// Resolves column names for all configured tables from INFORMATION_SCHEMA.
    /// Binlog row events don't include column names — only positional values.
    /// We need the column name list (in ordinal position order) to map values to names.
    /// </summary>
    private async Task ResolveColumnNames(CancellationToken ct)
    {
        var defaultSchema = GetDefaultSchema();
        var allTables = Configuration.CollectAllTablesFlat(defaultSchema);

        await using var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(ct);

        foreach (var tableInfo in allTables)
        {
            var columns = new List<ColumnInfo>();
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
                ORDER BY ORDINAL_POSITION";
            cmd.Parameters.AddWithValue("@schema", tableInfo.Schema);
            cmd.Parameters.AddWithValue("@table", tableInfo.TableName);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var colName = reader.GetString(0);
                var dataType = reader.GetString(1).ToLowerInvariant();
                var columnType = reader.GetString(2).ToLowerInvariant();
                // tinyint(1) is the canonical "boolean" shape in MySQL/MariaDB. MySqlConnector
                // returns CLR bool for it (TreatTinyAsBoolean=true is the default), but MySqlCdc's
                // binlog parser returns sbyte. Detecting the column type up front lets the
                // streaming pre-pass coerce sbyte/byte -> bool so both paths emit JSON booleans.
                // bit(1) is added defensively even though both paths already produce bool today.
                var category = (dataType, columnType) switch
                {
                    (_, "tinyint(1)") or (_, "tinyint(1) unsigned") => MySqlColumnCategory.Boolean,
                    ("bit", "bit(1)")                                => MySqlColumnCategory.Boolean,
                    ("json", _)                                      => MySqlColumnCategory.Json,
                    ("decimal" or "numeric", _)                      => MySqlColumnCategory.Decimal,
                    ("text" or "tinytext" or "mediumtext" or "longtext"
                        or "char" or "varchar" or "enum" or "set", _) => MySqlColumnCategory.Text,
                    _                                                => MySqlColumnCategory.Other,
                };
                columns.Add(new ColumnInfo(colName, category, dataType));
            }

            var columnsArray = columns.ToArray();

            var columnNames = new string[columnsArray.Length];
            for (int i = 0; i < columnsArray.Length; i++)
                columnNames[i] = columnsArray[i].Name;

            var processor = DocumentProcessor.GetProcessor(tableInfo.Schema, tableInfo.TableName);
            processor.SetSourceColumnNames(columnNames);

            // Build the expected type prefix covering all mapped column positions.
            // RequiredPrefixLength is computed by SetSourceColumnNames — it's the max
            // ordinal of any column we actually read (PK, mapped, join, attachment) + 1.
            var requiredLen = processor.RequiredPrefixLength;
            var expectedPrefix = new byte[requiredLen];
            for (int i = 0; i < requiredLen && i < columnsArray.Length; i++)
                expectedPrefix[i] = MapDataTypeToBinlogType(columnsArray[i].DataType);

            var tableKey = (tableInfo.Schema, tableInfo.TableName);
            _resolvedTables[tableKey] = new TableInfo
            {
                Columns = columnsArray,
                Processor = processor,
                ExpectedTypesPrefix = expectedPrefix,
            };
        }
    }

    /// <summary>
    /// MySQL sends HeartbeatEvent every 30s (HeartbeatInterval) even when the source is idle.
    /// These events fall through our switch statement but still update LastActivityTime.
    /// If LastActivityTime goes stale beyond ~90s (3x heartbeat), the binlog connection is dead.
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
        // HeartbeatInterval is 30s. Allow 3x before flagging.
        if (silentFor > TimeSpan.FromSeconds(90))
        {
            issue = $"No binlog activity for {silentFor.TotalSeconds:F0}s " +
                    "(expected heartbeat every 30s). " +
                    "The MySQL binlog connection may be dead.";
            return false;
        }

        return true;
    }

    protected override string GetDefaultSchema() => GetDefaultSchemaFromConnectionString(_connectionString);

    protected override async Task<DbConnection> OpenInitialLoadConnection(CancellationToken ct)
    {
        var conn = new MySqlConnection(_connectionString);
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

    protected override DbCommandBuilder CommandBuilder { get; } = new MySqlCommandBuilder();

    protected override Task BindKeysetParameters(DbCommand cmd, CdcSinkConfiguration.TableInfo tableInfo, List<string> pkColumns, string[] lastKeys, CancellationToken ct)
    {
        for (int i = 0; i < lastKeys.Length; i++)
            AddParameter(cmd, $"@k{i}", lastKeys[i]);
        return Task.CompletedTask;
    }

    protected override object ConvertInitialLoadValue(DbDataReader reader, int ordinal, CdcSinkConfiguration.TableInfo tableInfo)
    {
        var raw = reader.GetValue(ordinal);

        // For columns resolved as MySqlColumnCategory.Boolean (tinyint(1) / bit(1)), the two
        // libraries return different CLR shapes — MysqlConnector returns bool for tinyint(1) and
        // ulong for bit(N), MySqlCdc returns sbyte / byte / bool[1]. CoerceToBoolean handles all
        // of them and throws on anything else, so a future library change can't silently regress.
        // Null is filtered upstream by reader.IsDBNull in CdcSinkProcess.ReadOneBatch.
        if (IsBooleanColumn(tableInfo, ordinal, out var col))
            return CoerceToBoolean(raw, col);

        return ConvertMySqlValue(raw);
    }

    /// <summary>
    /// True if the column at <paramref name="ordinal"/> on the given table was resolved as
    /// MySqlColumnCategory.Boolean (i.e. tinyint(1) or bit(1)) during ResolveColumnNames.
    /// Returns false (without throwing) if the table is unresolved or the ordinal is out of range,
    /// since those represent "no metadata available, treat as a regular column".
    /// </summary>
    private bool IsBooleanColumn(CdcSinkConfiguration.TableInfo tableInfo, int ordinal, out ColumnInfo col)
    {
        if (_resolvedTables.TryGetValue((tableInfo.Schema, tableInfo.TableName), out var resolved)
            && ordinal < resolved.Columns.Length
            && resolved.Columns[ordinal].Category is MySqlColumnCategory.Boolean)
        {
            col = resolved.Columns[ordinal];
            return true;
        }
        col = default;
        return false;
    }

    /// <summary>
    /// Coerces a CLR value produced by MysqlConnector (initial-load) or MySqlCdc (streaming)
    /// for a column resolved as MySqlColumnCategory.Boolean into a CLR bool.
    ///
    /// THROWS on unrecognised CLR types — this is intentional. If a future MysqlConnector or
    /// MySqlCdc version produces a new CLR shape for Boolean-category columns, we want the CDC
    /// process to fault into its alert pipeline (CdcSinkProcessStatistics / CdcSinkNotifications)
    /// rather than silently stringify via ConvertMySqlValue's _ => value.ToString() fallback.
    /// The throw is the forcing function that drives the team to add explicit handling.
    ///
    /// Caller is responsible for upstream NULL filtering.
    /// </summary>
    private static bool CoerceToBoolean(object value, ColumnInfo col)
    {
        return value switch
        {
            bool b => b,
            sbyte sb => sb != 0,
            byte by => by != 0,
            ulong u => u != 0,
            bool[] arr when arr.Length == 1 => arr[0],
            _ => throw new InvalidOperationException(
                $"Unsupported CLR value type '{value.GetType().FullName}' for Boolean-category column " +
                $"'{col.Name}' (MySQL data type '{col.DataType}'). " +
                "Initial-load (MysqlConnector) is expected to return bool or ulong; " +
                "streaming (MySqlCdc) is expected to return sbyte, byte, or bool[1]. " +
                "If a new CLR shape has appeared, add explicit handling in CoerceToBoolean."),
        };
    }

    protected override async IAsyncEnumerable<CdcEvent> GetCdcEvents([EnumeratorCancellation] CancellationToken ct)
    {
        string savedGtid;
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var state = LoadState(context);
            savedGtid = state.LastLsn;
        }

        // No explicit GTID gap detection here — unlike PostgreSQL (replication slot) and SQL Server
        // (fn_cdc_get_min_lsn), MySQL gives a clear fatal error when the binlog client connects with
        // a GTID that references purged binlogs: "the master has purged binary logs containing GTIDs
        // that the slave requires". The retry loop surfaces this as a notification, and the admin can
        // reset the task state to trigger a fresh initial load.

        var csBuilder = new MySqlConnectionStringBuilder(_connectionString);

        var client = new BinlogClient(options =>
        {
                options.Hostname = csBuilder.Server;
                options.Port = (int)csBuilder.Port;
                options.Username = csBuilder.UserID;
                options.Password = csBuilder.Password;
                options.Database = csBuilder.Database;
                // Each binlog client needs a unique server_id in the MySQL replication topology.
                // We pick a random value from a ~2 billion range on every connection attempt.
                // Collision probability between two nodes is ~1/2B (birthday paradox is irrelevant
                // at these scales). If a collision does occur, MySQL kills the older connection
                // and lets the new one in. Since RunWithRetryAsync calls RunInternalAsync
                // on every retry, each attempt picks a fresh random ID, so collisions self-resolve
                // within one retry cycle.
                options.ServerId = Random.Shared.NextInt64(2, int.MaxValue);
                options.Blocking = true;
                options.HeartbeatInterval = TimeSpan.FromSeconds(30);
                options.SslMode = csBuilder.SslMode switch
                {
                    MySqlSslMode.Required or MySqlSslMode.VerifyCA or MySqlSslMode.VerifyFull
                        => SslMode.RequireVerifyCa,
                    MySqlSslMode.Preferred => SslMode.IfAvailable,
                    _ => SslMode.Disabled,
                };

                var gtidToResume = savedGtid ?? _serverGtid;
                if (string.IsNullOrEmpty(gtidToResume) == false)
                {
                    options.Binlog = _isMariaDb
                        ? BinlogOptions.FromGtid(GtidList.Parse(gtidToResume))
                        : BinlogOptions.FromGtid(GtidSet.Parse(gtidToResume));
                }
                else
                {
                    options.Binlog = BinlogOptions.FromStart();
                }
            });

            var defaultSchema = GetDefaultSchema();

            // MySQL streams binlog events for ALL databases on the server, not just ours.
            // Only cache TableMapEvents for configured tables in our database — row events
            // for uncached table IDs are skipped automatically by the TryGetValue check.
            var tableMapCache = new Dictionary<long, TableInfo>();

            await foreach (var (header, binlogEvent) in client.Replicate(ct))
            {
                // MySQL sends HeartbeatEvent every HeartbeatInterval (30s) when the source
                // we record them here to track connection liveness. 
                LastActivityTime = Database.Time.GetUtcNow();

                switch (binlogEvent)
                {
                    case TableMapEvent tableMap:
                        if (string.Equals(tableMap.DatabaseName, defaultSchema, StringComparison.OrdinalIgnoreCase)
                            && _resolvedTables.TryGetValue((tableMap.DatabaseName, tableMap.TableName), out var info))
                        {
                            // Schema change detection via TableId tracking + prefix comparison.
                            //
                            // MySQL assigns a new TableId on every ALTER TABLE. We compare the
                            // binlog column types prefix (covering only mapped positions) against
                            // our baseline from INFORMATION_SCHEMA.
                            //
                            // TableIdReplayingOldEvents (-1): replaying from old GTID, skip mismatches.
                            // TableIdExpectCurrentSchema (-2) or real TableId: throw on mismatch.

                            if (tableMap.TableId == info.ValidTableId)
                            {
                                // Same TableId as before — no schema change. Fast path.
                                tableMapCache[tableMap.TableId] = info;
                                break;
                            }

                            var expectedPrefix = info.ExpectedTypesPrefix;
                            var binlogTypes = tableMap.ColumnTypes;

                            if (binlogTypes.Length < expectedPrefix.Length)
                            {
                                // Not enough columns to cover mapped positions.
                                if (info.ValidTableId != TableIdReplayingOldEvents)
                                    throw new InvalidOperationException(
                                        $"Schema change detected for table {tableMap.DatabaseName}.{tableMap.TableName}: " +
                                        $"binlog event has {binlogTypes.Length} columns but mapped columns require at least {expectedPrefix.Length}. " +
                                        "The process will restart and re-resolve the schema.");

                                // No valid table seen yet — skip old-schema events
                                tableMapCache.Remove(tableMap.TableId);
                                if (Logger.IsInfoEnabled)
                                    Logger.Info($"[{Name}] Skipping old-schema TableMapEvent for {tableMap.DatabaseName}.{tableMap.TableName} " +
                                        $"({binlogTypes.Length} columns, need {expectedPrefix.Length}). Waiting for current-schema events.");
                                break;
                            }

                            if (expectedPrefix.AsSpan().SequenceEqual(binlogTypes.AsSpan(0, expectedPrefix.Length)))
                            {
                                // Prefix matches — all mapped positions are type-stable. Accept.
                                info.ValidTableId = tableMap.TableId;
                                tableMapCache[tableMap.TableId] = info;
                            }
                            else
                            {
                                if (info.ValidTableId != TableIdReplayingOldEvents)
                                    throw new InvalidOperationException(
                                        $"Schema change detected for table {tableMap.DatabaseName}.{tableMap.TableName}: " +
                                        $"column types changed in mapped region (positions 0..{expectedPrefix.Length - 1}). " +
                                        "The process will restart and re-resolve the schema.");

                                // No valid table seen yet — skip old-schema events
                                tableMapCache.Remove(tableMap.TableId);
                                if (Logger.IsInfoEnabled)
                                    Logger.Info($"[{Name}] Skipping old-schema TableMapEvent for {tableMap.DatabaseName}.{tableMap.TableName} " +
                                        "(column type prefix mismatch). Waiting for current-schema events.");
                            }
                        }
                        break;

                    case WriteRowsEvent writeRows:
                        if (tableMapCache.TryGetValue(writeRows.TableId, out var wTable) == false)
                            break;
                        foreach (var row in writeRows.Rows)
                            yield return new CdcEvent(CdcEventType.Upsert, DecodeRow(wTable, row.Cells, CdcSinkOperation.Upsert, StreamingJsonContext), null);
                        break;

                    case UpdateRowsEvent updateRows:
                        if (tableMapCache.TryGetValue(updateRows.TableId, out var uTable) == false)
                            break;
                        foreach (var row in updateRows.Rows)
                        {
                            var newOp = DecodeRow(uTable, row.AfterUpdate.Cells, CdcSinkOperation.Upsert, StreamingJsonContext);
                            if (newOp?.Processor is { IsRoot: false })
                            {
                                var oldValues = DecodeRowInternal(uTable, row.BeforeUpdate.Cells);
                                var (delete, upsert) = CreateEmbeddedUpdateEvents(newOp, oldValues);
                                if (delete.HasValue)
                                    yield return delete.Value;
                                else
                                    uTable.Processor.ReturnValues(oldValues); // no reparent — release the unused old-values array
                                yield return upsert;
                            }
                            else
                            {
                                yield return new CdcEvent(CdcEventType.Upsert, newOp, null);
                            }
                        }
                        break;

                    case DeleteRowsEvent deleteRows:
                        if (tableMapCache.TryGetValue(deleteRows.TableId, out var dTable) == false)
                            break;
                        foreach (var row in deleteRows.Rows)
                            yield return new CdcEvent(CdcEventType.Delete, DecodeRow(dTable, row.Cells, CdcSinkOperation.Delete, StreamingJsonContext), null);
                        break;

                    case XidEvent:
                        var gtid = client.State?.GtidState?.ToString()
                            ?? throw new InvalidOperationException("MySQL binlog client has no GTID state after XidEvent — GTID mode may not be enabled on the server.");
                        yield return new CdcEvent(CdcEventType.TransactionCommit, null, gtid);
                        break;
                }
            }
    }

    private CdcSinkDocumentOp DecodeRow(
        TableInfo tableInfo, IReadOnlyList<object> cells,
        CdcSinkOperation operation, DocumentsOperationContext jsonParsingContext)
    {
        var values = DecodeRowInternal(tableInfo, cells);
        return DocumentProcessor.ProcessRow(tableInfo.Processor, operation, values, jsonParsingContext);
    }

    /// <summary>
    /// Decodes binlog cells into raw column values and returns the decoded values array.
    /// Used by <see cref="DecodeRow"/> and by the reparent detection path
    /// (which needs the raw values without calling ProcessRow); the processor is available via <paramref name="tableInfo"/>.
    /// </summary>
    private object[] DecodeRowInternal(
        TableInfo tableInfo, IReadOnlyList<object> cells)
    {
        var columns = tableInfo.Columns;
        var processor = tableInfo.Processor;
        var values = processor.RentValues();
        for (int i = 0; i < columns.Length && i < cells.Count; i++)
        {
            var col = columns[i];
            var value = cells[i];

            values[i] = value switch
            {
                null or DBNull => null,

                // tinyint(1) / bit(1) columns. MySqlCdc returns sbyte for tinyint(1) and
                // bool[1] for bit(1); the initial-load path goes through CoerceToBoolean too,
                // keeping a single source of truth for "what counts as a booleanish CLR value".
                // CoerceToBoolean throws on unrecognised types so future library drift surfaces
                // as a loud CDC process error rather than silent stringification.
                var v when col.Category is MySqlColumnCategory.Boolean
                    => CoerceToBoolean(v, col),

                // MySqlCdc may return numeric types as strings (BCD-encoded internally).
                // Parse to decimal so they flow as numbers through the pipeline and patch scripts.
                string s when col.Category is MySqlColumnCategory.Decimal
                    && decimal.TryParse(s, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var dec)
                    => dec,

                // MySQL stores JSON in a proprietary binary format in the binlog.
                // Use MySqlCdc's JsonParser to convert to a JSON string.
                byte[] bytes when col.Category is MySqlColumnCategory.Json
                    => MySqlCdc.Providers.MySql.JsonParser.Parse(bytes),

                // MySQL's binlog uses the same BLOB type codes for TEXT and BLOB columns.
                // TEXT columns should be UTF-8 decoded; true binary columns pass through.
                byte[] bytes when col.Category is MySqlColumnCategory.Text
                    => System.Text.Encoding.UTF8.GetString(bytes),

                _ => ConvertMySqlValue(value),
            };
        }

        return values;
    }

    /// <summary>
    /// Normalizes MySQL CLR values from both ADO.NET (initial load) and MySqlCdc (binlog)
    /// to consistent types for the shared document processor pipeline.
    /// </summary>
    private static object ConvertMySqlValue(object value)
    {
        return value switch
        {
            null or DBNull => null,

            // Integer types → long for consistency
            sbyte sb => (long)sb,
            byte b => (long)b,
            short s => (long)s,
            ushort us => (long)us,
            int i => (long)i,
            uint ui => (long)ui,
            long l => l,
            ulong ul => ul,

            // Floating point — preserve precision
            float => value,
            double => value,
            decimal => value,

            // Boolean — only the initial-load path reaches this arm for booleans:
            // MySqlConnector returns CLR bool for TINYINT(1) and BIT(1) (TreatTinyAsBoolean=true
            // is its default). The streaming path coerces sbyte/byte -> bool earlier in DecodeRow
            // when the column was resolved as MySqlColumnCategory.Boolean, so it never reaches here.
            bool => value,

            // Date/Time
            DateTime => value,
            DateOnly => value,
            TimeSpan ts => ts.ToString(), // TIME type → string to avoid precision loss
            DateTimeOffset => value,

            // Strings pass through
            string => value,

            // Binary data (BLOB, VARBINARY)
            byte[] => value,

            // GUID
            Guid g => g.ToString(),

            // MySqlCdc may return SET values as long (bitmask) — we handle string SET values above.
            // For SET as bitmask, we'd need the SET definition to decode — fall through to ToString.

            // Everything else → string
            _ => value.ToString(),
        };
    }

}
