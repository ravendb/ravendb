using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MySqlCdc;
using MySqlCdc.Constants;
using MySqlCdc.Events;
using MySqlCdc.Providers.MariaDb;
using MySqlCdc.Providers.MySql;
using MySqlConnector;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.CdcSink.Stats;
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
    private long _serverId;     // Unique server ID for binlog replication, selected at runtime

    private readonly record struct ColumnInfo(string Name, bool IsText);

    // Cache column metadata per table (database.table → column info array).
    // Binlog row events don't include column names — only positional values.
    // We also track whether each column is text-based because MySQL's binlog uses
    // the same BLOB type codes for TEXT and BLOB columns; text columns need UTF-8
    // decoding from byte[]. Resolved from INFORMATION_SCHEMA.COLUMNS during startup.
    private readonly Dictionary<string, ColumnInfo[]> _tableColumns = new(StringComparer.OrdinalIgnoreCase);

    public MySqlCdcSinkProcess(CdcSinkConfiguration configuration, DocumentDatabase database)
        : base(configuration, database)
    {
        _connectionString = configuration.Connection.ConnectionString;
    }

    protected override async Task RunInternalAsync(CancellationToken ct)
    {
        await EnsureBinlogConfiguration(ct);
        await ResolveColumnNames(ct);
        await HandleInitialLoad(ct);
        _initialLoadTcs.TrySetResult();
        await StartListening(ct);
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

        // Select a unique server ID for binlog replication. The ID must not conflict
        // with the source server or any other replica currently connected.
        _serverId = await SelectUniqueServerId(conn, ct);
    }

    /// <summary>
    /// Selects a random server ID that doesn't conflict with the source server or existing replicas.
    /// MySQL requires each replica to have a unique server_id (range 1 to 2^32-1). If our ID matches
    /// the source server, MySQL filters out the very events we're trying to capture.
    /// </summary>
    private async Task<long> SelectUniqueServerId(MySqlConnection conn, CancellationToken ct)
    {
        var usedIds = new List<long>();

        // Get the source server's own ID — we must NOT use this one
        await using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT @@server_id";
            var result = await cmd.ExecuteScalarAsync(ct);
            if (result != null && result != DBNull.Value)
                usedIds.Add(Convert.ToInt64(result));
        }

        // Get IDs of other replicas currently connected.
        // Try SHOW SLAVE HOSTS first (works on all MySQL versions including 8.4 where
        // it's deprecated but still functional). Fall back to SHOW REPLICAS (MySQL 8.0.22+)
        // if the older syntax is removed in a future version.
        if (await TryCollectReplicaIds(conn, "SHOW SLAVE HOSTS", usedIds, ct) == false)
            await TryCollectReplicaIds(conn, "SHOW REPLICAS", usedIds, ct);

        // Find the first gap in the sorted ID list, starting from 2 (1 is often the source server)
        usedIds.Sort();
        long candidate = 2;
        foreach (var id in usedIds)
        {
            if (id > candidate)
                break;
            if (id >= candidate)
                candidate = id + 1; // id+1 for [1, 1, 2, 2, 5] handling
        }
        return candidate;
    }

    private static async Task<bool> TryCollectReplicaIds(MySqlConnection conn, string command, List<long> usedIds, CancellationToken ct)
    {
        try
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = command;
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
                usedIds.Add(reader.GetInt64(0)); // Server_id is the first column
            return true;
        }
        catch (MySqlException)
        {
            return false;
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
            var tableKey = $"{tableInfo.Schema}.{tableInfo.TableName}";
            if (_tableColumns.ContainsKey(tableKey))
                continue;

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
                var isText = dataType is "text" or "tinytext" or "mediumtext" or "longtext"
                    or "char" or "varchar" or "enum" or "set" or "json";
                columns.Add(new ColumnInfo(colName, isText));
            }

            _tableColumns[tableKey] = columns.ToArray();
        }
    }

    protected override string GetDefaultSchema()
    {
        var builder = new MySqlConnectionStringBuilder(_connectionString);
        return builder.Database ?? "mysql";
    }

    #region Initial Load


    protected override async Task<DbConnection> OpenInitialLoadConnection(CancellationToken ct)
    {
        var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync(ct);
        return conn;
    }

    protected override char StartQuote => '`';
    protected override char EndQuote => '`';

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

    #endregion

    #region Binlog Streaming

    private async Task StartListening(CancellationToken ct)
    {
        string savedGtid;
        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var state = LoadState(context);
            savedGtid = state.LastLsn;
        }

        // Parse connection string to extract host/port/credentials for the binlog client
        var csBuilder = new MySqlConnectionStringBuilder(_connectionString);

        using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext jsonParsingContext))
        {
            var client = new BinlogClient(options =>
            {
                options.Hostname = csBuilder.Server;
                options.Port = (int)csBuilder.Port;
                options.Username = csBuilder.UserID;
                options.Password = csBuilder.Password;
                options.Database = csBuilder.Database;
                options.ServerId = _serverId;
                options.Blocking = true;
                options.HeartbeatInterval = TimeSpan.FromSeconds(30);
                options.SslMode = csBuilder.SslMode switch
                {
                    MySqlSslMode.Required or MySqlSslMode.VerifyCA or MySqlSslMode.VerifyFull
                        => MySqlCdc.Constants.SslMode.RequireVerifyCa,
                    MySqlSslMode.Preferred => MySqlCdc.Constants.SslMode.IfAvailable,
                    _ => MySqlCdc.Constants.SslMode.Disabled,
                };

                // Resume from saved GTID or start from the server's current GTID position
                // (fetched during EnsureBinlogConfiguration).
                var gtidToResume = savedGtid ?? _serverGtid;
                if (string.IsNullOrEmpty(gtidToResume) == false)
                {
                    options.Binlog = _isMariaDb
                        ? BinlogOptions.FromGtid(GtidList.Parse(gtidToResume))
                        : BinlogOptions.FromGtid(GtidSet.Parse(gtidToResume));
                }
                else
                {
                    // No GTID available — start from beginning (rare edge case)
                    options.Binlog = BinlogOptions.FromStart();
                }
            });

            var batch = new List<CdcSinkDocumentOp>();
            var pending = new List<CdcSinkDocumentOp>();
            Task lastBatch = Task.CompletedTask;
            int maxBatchSize = Database.Configuration.CdcSink.MaxBatchSize;

            // TableMapEvent cache: tableId → (database, table, columnTypes)
            var tableMapCache = new Dictionary<long, (string Database, string Table, byte[] ColumnTypes)>();

            await foreach (var (header, binlogEvent) in client.Replicate(ct))
            {
                ct.ThrowIfCancellationRequested();

                switch (binlogEvent)
                {
                    case TableMapEvent tableMap:
                        tableMapCache[tableMap.TableId] = (tableMap.DatabaseName, tableMap.TableName, tableMap.ColumnTypes);
                        break;

                    case WriteRowsEvent writeRows:
                        if (tableMapCache.TryGetValue(writeRows.TableId, out var wTable) == false)
                            break;
                        foreach (var row in writeRows.Rows)
                            pending.Add(DecodeRow(wTable.Database, wTable.Table, row.Cells, wTable.ColumnTypes, CdcSinkOperation.Upsert, jsonParsingContext));
                        break;

                    case UpdateRowsEvent updateRows:
                        if (tableMapCache.TryGetValue(updateRows.TableId, out var uTable) == false)
                            break;
                        foreach (var row in updateRows.Rows)
                            pending.Add(DecodeRow(uTable.Database, uTable.Table, row.AfterUpdate.Cells, uTable.ColumnTypes, CdcSinkOperation.Upsert, jsonParsingContext));
                        break;

                    case DeleteRowsEvent deleteRows:
                        if (tableMapCache.TryGetValue(deleteRows.TableId, out var dTable) == false)
                            break;
                        foreach (var row in deleteRows.Rows)
                            pending.Add(DecodeRow(dTable.Database, dTable.Table, row.Cells, dTable.ColumnTypes, CdcSinkOperation.Delete, jsonParsingContext));
                        break;

                    case XidEvent:
                        // Transaction committed — move pending rows to the batch
                        batch.AddRange(pending);
                        pending.Clear();

                        if (lastBatch.IsCompleted || ShouldFlushBatch(batch.Count))
                        {
                            await lastBatch;

                            if (batch.Count > 0)
                            {
                                // Save GTID state as checkpoint — it's an opaque string
                                var gtidCheckpoint = client.State?.ToString();
                                lastBatch = SubmitBatch(batch, gtidCheckpoint);
                                batch = new List<CdcSinkDocumentOp>();
                            }
                        }
                        break;
                }
            }
        }
    }

    private CdcSinkDocumentOp DecodeRow(
        string database, string tableName,
        IReadOnlyList<object> cells, byte[] columnTypes,
        CdcSinkOperation operation,
        DocumentsOperationContext jsonParsingContext)
    {
        var tableKey = $"{database}.{tableName}";
        if (_tableColumns.TryGetValue(tableKey, out var columns) == false)
            return null;

        var data = new Dictionary<string, object>(columns.Length);
        for (int i = 0; i < columns.Length && i < cells.Count; i++)
        {
            var col = columns[i];
            var value = cells[i];
            if (value == null)
            {
                data[col.Name] = null;
                continue;
            }

            if (value is byte[] bytes)
            {
                // MySQL stores JSON in a proprietary binary format in the binlog.
                // Use MySqlCdc's JsonParser to convert to a JSON string.
                if (columnTypes != null && i < columnTypes.Length && columnTypes[i] == (byte)ColumnType.Json)
                {
                    value = MySqlCdc.Providers.MySql.JsonParser.Parse(bytes);
                }
                // MySQL's binlog uses the same BLOB type codes for TEXT and BLOB columns.
                // TEXT columns should be UTF-8 decoded; true binary columns pass through.
                else if (col.IsText)
                {
                    value = System.Text.Encoding.UTF8.GetString(bytes);
                }
            }

            data[col.Name] = ConvertMySqlValue(value);
        }

        var row = new CdcSinkRow
        {
            TableSchema = database,
            TableName = tableName,
            Operation = operation,
            Data = data,
        };

        return DocumentProcessor.ProcessRow(row, jsonParsingContext);
    }

    #endregion

    #region Type Conversion

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
            ulong ul => (long)ul,

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
            DateTimeOffset dto => dto.DateTime,

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

    #endregion
}
