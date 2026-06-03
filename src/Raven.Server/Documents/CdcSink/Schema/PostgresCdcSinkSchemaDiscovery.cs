using System;
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
                    // information_schema reports IS_GENERATED as 'ALWAYS' for STORED generated columns,
                    // 'NEVER' otherwise. Generated columns are not published over logical replication,
                    // so CDC cannot deliver them.
                    var isGenerated = string.Equals(reader["IS_GENERATED"]?.ToString(), "ALWAYS", StringComparison.OrdinalIgnoreCase);

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

            return schema;
        }

        /// <summary>
        /// Postgres CDC stores values per category at runtime (<see cref="PostgresColumnTypeMapping"/>),
        /// but at discovery time we only have the INFORMATION_SCHEMA <c>data_type</c> string. Map the
        /// well-known JSON / BYTEA cases here so Studio can pre-populate the column-mapping UI;
        /// everything else defaults to <see cref="CdcColumnType.Default"/>.
        /// </summary>
        private const string GeneratedColumnReason =
            "Column is generated/computed by the source database and is not emitted by CDC.";

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
            var keyColumnUsage = new Dictionary<string, (string Schema, string Table, List<string> Columns)>();
            await using (var cmd = new NpgsqlCommand(queries.SelectKeyColumnUsageQuery, conn))
            await using (var reader = await cmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    var constraintName = reader["CONSTRAINT_NAME"].ToString();
                    var schemaName = reader["TABLE_SCHEMA"].ToString();
                    var tableName = reader["TABLE_NAME"].ToString();
                    var columnName = reader["COLUMN_NAME"].ToString();

                    if (keyColumnUsage.TryGetValue(constraintName, out var entry) == false)
                    {
                        entry = (schemaName, tableName, new List<string>());
                        keyColumnUsage[constraintName] = entry;
                    }
                    entry.Columns.Add(columnName);
                }
            }

            await using var refCmd = new NpgsqlCommand(queries.SelectReferentialConstraintsQuery, conn);
            await using var refReader = await refCmd.ExecuteReaderAsync(ct);
            while (await refReader.ReadAsync(ct))
            {
                var fkConstraint = refReader["CONSTRAINT_NAME"].ToString();
                var uniqueConstraint = refReader["UNIQUE_CONSTRAINT_NAME"].ToString();

                if (keyColumnUsage.TryGetValue(fkConstraint, out var fkEntry) == false)
                    continue;
                if (keyColumnUsage.TryGetValue(uniqueConstraint, out var pkEntry) == false)
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
