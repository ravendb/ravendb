using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Server.SqlMigration.NpgSQL;

namespace Raven.Server.Documents.CdcSink.Schema
{
    internal sealed class PostgresCdcSinkSchemaDiscovery : CdcSinkSchemaDiscovery
    {
        public override async Task<CdcSinkSourceSchema> DiscoverAsync(string connectionString, string[] schemas, CancellationToken ct)
        {
            var queries = new NpgSqlSchemaQueries(schemas);

            await using var conn = new NpgsqlConnection(connectionString);
            await conn.OpenAsync(ct);

            var schema = new CdcSinkSourceSchema { CatalogName = conn.Database };
            var tableLookup = new Dictionary<(string Schema, string Table), CdcSinkSourceTable>();
            var columnLookup = new Dictionary<(string Schema, string Table, string Column), CdcSinkSourceColumn>();

            await using (var cmd = new NpgsqlCommand(queries.SelectColumnsQuery, conn))
            await using (var reader = await cmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var schemaName = reader["TABLE_SCHEMA"].ToString();
                    var tableName = reader["TABLE_NAME"].ToString();
                    var columnName = reader["COLUMN_NAME"].ToString();
                    var nativeType = reader["DATA_TYPE"].ToString().ToLowerInvariant();

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
                        IsCdcCapturable = true,
                    };
                    table.Columns.Add(column);
                    columnLookup[(schemaName, tableName, columnName)] = column;
                }
            }

            await using (var cmd = new NpgsqlCommand(queries.SelectPrimaryKeysQuery, conn))
            await using (var reader = await cmd.ExecuteReaderAsync(ct))
            {
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

            await PopulateOutgoingForeignKeysAsync(conn, queries, tableLookup, ct);
            await ApplyGeneratedColumnsAsync(conn, columnLookup, ct);

            return schema;
        }

        /// <summary>
        /// Marks STORED generated columns as not CDC-capturable. Postgres does not publish generated
        /// columns over logical replication (before PG 18), so CDC cannot deliver them. The
        /// <c>information_schema.columns.is_generated</c> column only exists on PostgreSQL 12+, so this
        /// pass is skipped on older servers - which have no generated columns anyway - and keeps the
        /// shared <see cref="NpgSqlSchemaQueries.SelectColumnsQuery"/> free of version-specific columns
        /// (it is also used by the SQL Migration feature, which supports PG 10/11).
        /// </summary>
        private static async Task ApplyGeneratedColumnsAsync(
            NpgsqlConnection conn,
            Dictionary<(string Schema, string Table, string Column), CdcSinkSourceColumn> columnLookup,
            CancellationToken ct)
        {
            if (conn.PostgreSqlVersion.Major < 12)
                return;

            await using var cmd = new NpgsqlCommand(
                "SELECT table_schema, table_name, column_name FROM information_schema.columns WHERE is_generated = 'ALWAYS'", conn);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var key = (reader["table_schema"].ToString(), reader["table_name"].ToString(), reader["column_name"].ToString());
                if (columnLookup.TryGetValue(key, out var column))
                {
                    column.IsCdcCapturable = false;
                    column.UnsupportedReason = GeneratedColumnReason;
                }
            }
        }

        /// <summary>
        /// Postgres CDC stores values per category at runtime (<see cref="PostgresColumnTypeMapping"/>),
        /// but at discovery time we only have the INFORMATION_SCHEMA <c>data_type</c> string. Map the
        /// well-known JSON / BYTEA cases here so Studio can pre-populate the column-mapping UI;
        /// everything else defaults to <see cref="CdcColumnType.Default"/>.
        /// </summary>
        private static CdcColumnType SuggestType(string lowerNativeType)
        {
            return lowerNativeType switch
            {
                "json" or "jsonb" => CdcColumnType.Json,
                "bytea" => CdcColumnType.Attachment,
                _ => CdcColumnType.Default,
            };
        }

        private static async Task PopulateOutgoingForeignKeysAsync(
            NpgsqlConnection conn,
            NpgSqlSchemaQueries queries,
            Dictionary<(string Schema, string Table), CdcSinkSourceTable> tableLookup,
            CancellationToken ct)
        {
            // Key by (schema, constraint name): constraint names are unique only within a schema,
            // so a bare-name key merges/overwrites identically-named constraints across schemas.
            var keyColumnUsage = new Dictionary<(string Schema, string ConstraintName), (string Schema, string Table, List<string> Columns)>();
            await using (var cmd = new NpgsqlCommand(queries.SelectKeyColumnUsageQuery, conn))
            await using (var reader = await cmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var constraintName = reader["CONSTRAINT_NAME"].ToString();
                    var schemaName = reader["TABLE_SCHEMA"].ToString();
                    var tableName = reader["TABLE_NAME"].ToString();
                    var columnName = reader["COLUMN_NAME"].ToString();

                    if (keyColumnUsage.TryGetValue((schemaName, constraintName), out var entry) == false)
                    {
                        entry = (schemaName, tableName, new List<string>());
                        keyColumnUsage[(schemaName, constraintName)] = entry;
                    }
                    entry.Columns.Add(columnName);
                }
            }

            await using var refCmd = new NpgsqlCommand(queries.SelectReferentialConstraintsQuery, conn);
            await using var refReader = await refCmd.ExecuteReaderAsync(ct);
            while (await refReader.ReadAsync(ct))
            {
                var fkSchema = refReader["CONSTRAINT_SCHEMA"].ToString();
                var fkConstraint = refReader["CONSTRAINT_NAME"].ToString();
                var uniqueSchema = refReader["UNIQUE_CONSTRAINT_SCHEMA"].ToString();
                var uniqueConstraint = refReader["UNIQUE_CONSTRAINT_NAME"].ToString();

                if (keyColumnUsage.TryGetValue((fkSchema, fkConstraint), out var fkEntry) == false)
                    continue;
                if (keyColumnUsage.TryGetValue((uniqueSchema, uniqueConstraint), out var pkEntry) == false)
                    continue;
                if (tableLookup.TryGetValue((fkEntry.Schema, fkEntry.Table), out var fkTable) == false)
                    continue;

                fkTable.ForeignKeys.Add(new CdcSinkSourceForeignKey
                {
                    Columns = fkEntry.Columns,
                    ReferencedSchema = pkEntry.Schema,
                    ReferencedTable = pkEntry.Table,
                    ReferencedColumns = pkEntry.Columns,
                });
            }
        }
    }
}
