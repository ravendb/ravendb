using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Server.SqlMigration.MySQL;

namespace Raven.Server.Documents.CdcSink.Schema
{
    internal sealed class MySqlCdcSinkSchemaDiscovery : CdcSinkSchemaDiscovery
    {
        private static readonly MySqlSchemaQueries Queries = new MySqlSchemaQueries();

        public override async Task<CdcSinkSourceSchema> DiscoverAsync(string connectionString, string[] schemas, CancellationToken ct)
        {
            await using var conn = new MySqlConnection(connectionString);
            await conn.OpenAsync(ct);

            var schema = new CdcSinkSourceSchema { CatalogName = conn.Database };
            var tableLookup = new Dictionary<(string Schema, string Table), CdcSinkSourceTable>();
            var columnLookup = new Dictionary<(string Schema, string Table, string Column), CdcSinkSourceColumn>();

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = Queries.SelectColumnsQuery;
                Queries.AddSchemaParameter(cmd, conn);

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var schemaName = reader["TABLE_SCHEMA"].ToString();
                    var tableName = reader["TABLE_NAME"].ToString();
                    var columnName = reader["COLUMN_NAME"].ToString();
                    var nativeType = reader["DATA_TYPE"].ToString().ToLowerInvariant();
                    var isGenerated = IsGeneratedColumn(reader["EXTRA"]?.ToString());

                    if (tableLookup.TryGetValue((schemaName, tableName), out var table) == false)
                    {
                        table = new CdcSinkSourceTable
                        {
                            SourceTableSchema = schemaName,
                            SourceTableName = tableName,
                            IsCdcEnabled = true,
                        };
                        tableLookup[(schemaName, tableName)] = table;
                        schema.Tables.Add(table);
                    }

                    var column = new CdcSinkSourceColumn
                    {
                        Name = columnName,
                        NativeType = nativeType,
                        SuggestedType = SuggestType(nativeType),
                        IsCdcCapturable = isGenerated == false,
                        UnsupportedReason = isGenerated ? GeneratedColumnReason : null,
                    };
                    table.Columns.Add(column);
                    columnLookup[(schemaName, tableName, columnName)] = column;
                }
            }

            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = Queries.SelectPrimaryKeysQuery;
                Queries.AddSchemaParameter(cmd, conn);

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var schemaName = reader["TABLE_SCHEMA"].ToString();
                    var tableName = reader["TABLE_NAME"].ToString();
                    var columnName = reader["COLUMN_NAME"].ToString();

                    if (tableLookup.TryGetValue((schemaName, tableName), out var table) == false)
                        continue;

                    table.PrimaryKeyColumns.Add(columnName);
                    if (columnLookup.TryGetValue((schemaName, tableName, columnName), out var column))
                        column.IsPrimaryKey = true;
                }
            }

            await PopulateOutgoingForeignKeysAsync(conn, tableLookup, ct);

            return schema;
        }

        /// <summary>
        /// MySQL's <c>SelectKeyColumnUsageQuery</c> returns one row per (FK column, referenced
        /// column) pair, scoped to a single schema via <c>@schema</c>. The cache is keyed on
        /// <c>"schema:constraint"</c> to handle constraint-name collisions across schemas.
        /// <c>SelectReferentialConstraintsQuery</c> provides the FK's table + referenced table directly.
        /// </summary>
        private static async Task PopulateOutgoingForeignKeysAsync(
            MySqlConnection conn,
            Dictionary<(string Schema, string Table), CdcSinkSourceTable> tableLookup,
            CancellationToken ct)
        {
            var keyColumnUsage = new Dictionary<string, (List<string> Columns, List<string> ReferencedColumns)>();
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = Queries.SelectKeyColumnUsageQuery;
                Queries.AddSchemaParameter(cmd, conn);

                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    var cacheKey = reader["CONSTRAINT_SCHEMA"] + ":" + reader["CONSTRAINT_NAME"];
                    var columnName = reader["COLUMN_NAME"].ToString();
                    var referencedColumnName = reader["REFERENCED_COLUMN_NAME"].ToString();

                    if (keyColumnUsage.TryGetValue(cacheKey, out var entry) == false)
                    {
                        entry = (new List<string>(), new List<string>());
                        keyColumnUsage[cacheKey] = entry;
                    }
                    entry.Columns.Add(columnName);
                    entry.ReferencedColumns.Add(referencedColumnName);
                }
            }

            await using var refCmd = conn.CreateCommand();
            refCmd.CommandText = Queries.SelectReferentialConstraintsQuery;
            Queries.AddSchemaParameter(refCmd, conn);
            await using var refReader = await refCmd.ExecuteReaderAsync(ct);
            while (await refReader.ReadAsync(ct))
            {
                var cacheKey = refReader["CONSTRAINT_SCHEMA"] + ":" + refReader["CONSTRAINT_NAME"];
                if (keyColumnUsage.TryGetValue(cacheKey, out var entry) == false)
                    continue;

                var fkSchema = refReader["CONSTRAINT_SCHEMA"].ToString();
                var fkTableName = refReader["TABLE_NAME"].ToString();
                var pkSchema = refReader["UNIQUE_CONSTRAINT_SCHEMA"].ToString();
                var pkTableName = refReader["REFERENCED_TABLE_NAME"].ToString();

                if (tableLookup.TryGetValue((fkSchema, fkTableName), out var fkTable) == false)
                    continue;

                fkTable.ForeignKeys.Add(new CdcSinkSourceForeignKey
                {
                    Columns = entry.Columns,
                    ReferencedSchema = pkSchema,
                    ReferencedTable = pkTableName,
                    ReferencedColumns = entry.ReferencedColumns,
                });
            }
        }

        /// <summary>
        /// True for a MySQL/MariaDB generated (computed) column, read from INFORMATION_SCHEMA.COLUMNS.EXTRA.
        /// EXTRA is present on every server version (unlike GENERATION_EXPRESSION, which is MySQL 5.7.6+),
        /// keeping the shared SQL Migration query backward-compatible. MySQL 5.7.6+ / MariaDB 10.2+ report
        /// "STORED GENERATED" / "VIRTUAL GENERATED"; older MariaDB reports standalone "VIRTUAL" / "PERSISTENT".
        /// The space-delimited tokens deliberately exclude MySQL 8.0.13+'s "DEFAULT_GENERATED" marker, which
        /// is set on ordinary expression-default columns that are present in the binlog row image and are
        /// fully CDC-capturable.
        /// </summary>
        private static bool IsGeneratedColumn(string extra)
        {
            if (string.IsNullOrEmpty(extra))
                return false;

            return extra.Contains("STORED GENERATED", StringComparison.OrdinalIgnoreCase)
                || extra.Contains("VIRTUAL GENERATED", StringComparison.OrdinalIgnoreCase)
                || extra.Equals("VIRTUAL", StringComparison.OrdinalIgnoreCase)
                || extra.Equals("PERSISTENT", StringComparison.OrdinalIgnoreCase);
        }

        private static CdcColumnType SuggestType(string lowerNativeType)
        {
            return lowerNativeType switch
            {
                "json" => CdcColumnType.Json,
                "blob" or "longblob" or "mediumblob" or "tinyblob" or "binary" or "varbinary" => CdcColumnType.Attachment,
                _ => CdcColumnType.Default,
            };
        }
    }
}
