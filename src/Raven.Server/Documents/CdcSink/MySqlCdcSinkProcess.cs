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

    private enum MySqlColumnCategory { Other, Text, Decimal, Json }

    private readonly record struct ColumnInfo(string Name, MySqlColumnCategory Category);

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
        /// Column type codes from the first TableMapEvent for this table.
        /// Used to detect schema changes (column reorders, type changes) mid-stream.
        /// </summary>
        public byte[] BinlogColumnTypes { get; set; }
    }

    private readonly Dictionary<(string Schema, string Table), TableInfo> _resolvedTables = new(TableKeyComparer.Instance);

    public MySqlCdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
        : base(configuration, database)
    {
        _connectionString = configuration.Connection.ConnectionString;
    }

    protected override async Task RunInternalAsync(CancellationToken ct)
    {
        _resolvedTables.Clear();
        await EnsureBinlogConfiguration(ct);
        await ResolveColumnNames(ct);
        await HandleInitialLoad(ct);
        _initialLoadTcs.TrySetResult();
        await ProcessCdcStream(ct);
    }

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

        // Fetch the current server GTID set so we can start streaming from this point
        // after the initial load completes. We use FromGtid() instead of FromEnd()
        // because MySqlCdc's FromEnd() internally executes SHOW MASTER STATUS, which
        // was removed in MySQL 8.4 (replaced by SHOW BINARY LOG STATUS).
        // See: https://dev.mysql.com/doc/refman/8.4/en/show-binary-log-status.html
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
                SELECT COLUMN_NAME, DATA_TYPE
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
                var category = dataType switch
                {
                    "json" => MySqlColumnCategory.Json,
                    "decimal" or "numeric" => MySqlColumnCategory.Decimal,
                    "text" or "tinytext" or "mediumtext" or "longtext"
                        or "char" or "varchar" or "enum" or "set" => MySqlColumnCategory.Text,
                    _ => MySqlColumnCategory.Other,
                };
                columns.Add(new ColumnInfo(colName, category));
            }

            var columnsArray = columns.ToArray();

            var columnNames = new string[columnsArray.Length];
            for (int i = 0; i < columnsArray.Length; i++)
                columnNames[i] = columnsArray[i].Name;

            var processor = DocumentProcessor.GetProcessor(tableInfo.Schema, tableInfo.TableName);
            processor.SetSourceColumnNames(columnNames);

            var tableKey = (tableInfo.Schema, tableInfo.TableName);
            _resolvedTables[tableKey] = new TableInfo
            {
                Columns = columnsArray,
                Processor = processor,
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

    protected override string GetDefaultSchema()
    {
        var builder = new MySqlConnectionStringBuilder(_connectionString);
        return builder.Database ?? "mysql";
    }

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

    protected override object ConvertInitialLoadValue(DbDataReader reader, int ordinal)
    {
        return ConvertMySqlValue(reader.GetValue(ordinal));
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
                            if (info.BinlogColumnTypes == null)
                            {
                                // First TableMapEvent for this table — store the column types
                                // for future schema change detection.
                                if (tableMap.ColumnTypes.Length != info.Columns.Length)
                                {
                                    throw new InvalidOperationException(
                                        $"Column count mismatch for table {tableMap.DatabaseName}.{tableMap.TableName}: " +
                                        $"expected {info.Columns.Length} columns but binlog reports {tableMap.ColumnTypes.Length}. " +
                                        "The table schema may have changed. The process will retry with updated column metadata.");
                                }
                                info.BinlogColumnTypes = [.. tableMap.ColumnTypes];
                            }
                            else if (info.BinlogColumnTypes.AsSpan().SequenceEqual(tableMap.ColumnTypes) == false)
                            {
                                // Schema changed mid-stream: column count, types, or order differ.
                                // Force a restart to re-learn the schema.
                                throw new InvalidOperationException(
                                    $"Schema change detected for table {tableMap.DatabaseName}.{tableMap.TableName}: " +
                                    $"binlog column types no longer match the cached metadata. " +
                                    "The process will retry with updated column metadata.");
                            }
                            tableMapCache[tableMap.TableId] = info;
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
                            yield return new CdcEvent(CdcEventType.Upsert, DecodeRow(uTable, row.AfterUpdate.Cells, CdcSinkOperation.Upsert, StreamingJsonContext), null);
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

        return DocumentProcessor.ProcessRow(processor, operation, values, jsonParsingContext);
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

            // Boolean (MySQL TINYINT(1) or BIT(1))
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
