using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.CdcSink.Schema;
using Raven.Server.SqlMigration.MsSQL;

namespace Raven.Server.Documents.CdcSink.Schema
{
    internal sealed class SqlServerCdcSinkSchemaDiscovery : CdcSinkSchemaDiscovery
    {
        private static readonly MsSqlSchemaQueries Queries = new MsSqlSchemaQueries();

        public override async Task<CdcSinkSourceSchema> DiscoverAsync(string connectionString, string[] schemas, CancellationToken ct)
        {
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);

            var schema = new CdcSinkSourceSchema { CatalogName = conn.Database };
            var tableLookup = new Dictionary<(string Schema, string Table), CdcSinkSourceTable>();
            var columnLookup = new Dictionary<(string Schema, string Table, string Column), CdcSinkSourceColumn>();

            await ReadColumnsAsync(conn, schema, tableLookup, columnLookup, ct);
            await ReadPrimaryKeysAsync(conn, tableLookup, columnLookup, ct);
            await PopulateOutgoingForeignKeysAsync(conn, tableLookup, ct);
            await ApplyCdcEnrollmentAsync(conn, tableLookup, columnLookup, ct);

            return schema;
        }

        private static async Task ReadColumnsAsync(
            DbConnection conn,
            CdcSinkSourceSchema schema,
            Dictionary<(string Schema, string Table), CdcSinkSourceTable> tableLookup,
            Dictionary<(string Schema, string Table, string Column), CdcSinkSourceColumn> columnLookup,
            CancellationToken ct)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = Queries.SelectColumnsQuery;

            await using var reader = await cmd.ExecuteReaderAsync(ct);
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
                    };
                    tableLookup[(schemaName, tableName)] = table;
                    schema.Tables.Add(table);
                }

                var column = new CdcSinkSourceColumn
                {
                    Name = columnName,
                    NativeType = nativeType,
                    SuggestedType = SuggestType(nativeType),
                };
                table.Columns.Add(column);
                columnLookup[(schemaName, tableName, columnName)] = column;
            }
        }

        private static async Task ReadPrimaryKeysAsync(
            DbConnection conn,
            Dictionary<(string Schema, string Table), CdcSinkSourceTable> tableLookup,
            Dictionary<(string Schema, string Table, string Column), CdcSinkSourceColumn> columnLookup,
            CancellationToken ct)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = Queries.SelectPrimaryKeysQuery;

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

        private static async Task PopulateOutgoingForeignKeysAsync(
            DbConnection conn,
            Dictionary<(string Schema, string Table), CdcSinkSourceTable> tableLookup,
            CancellationToken ct)
        {
            var keyColumnUsage = new Dictionary<string, (string Schema, string Table, List<string> Columns)>();
            await using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = Queries.SelectKeyColumnUsageQuery;
                await using var reader = await cmd.ExecuteReaderAsync(ct);
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

            await using var refCmd = conn.CreateCommand();
            refCmd.CommandText = Queries.SelectReferentialConstraintsQuery;
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

        /// <summary>
        /// Cross-references the table / column list against the <c>cdc.*</c> catalog so Studio
        /// can disable or warn on tables that aren't actually CDC-tracked and columns that
        /// CDC won't emit (because they weren't included in the capture instance's column list).
        /// </summary>
        private static async Task ApplyCdcEnrollmentAsync(
            DbConnection conn,
            Dictionary<(string Schema, string Table), CdcSinkSourceTable> tableLookup,
            Dictionary<(string Schema, string Table, string Column), CdcSinkSourceColumn> columnLookup,
            CancellationToken ct)
        {
            // The cdc.* catalog views only exist after sys.sp_cdc_enable_db. If CDC has never been
            // turned on for the database, mark every table as not-CDC-enabled and bail out before
            // we hit a "Invalid object name 'cdc.change_tables'" error.
            if (await IsCdcEnabledForDatabaseAsync(conn, ct) == false)
            {
                foreach (var table in tableLookup.Values)
                {
                    table.IsCdcEnabled = false;
                    foreach (var column in table.Columns)
                    {
                        column.IsCdcCapturable = false;
                        column.UnsupportedReason = "CDC is not enabled on the source database (sys.sp_cdc_enable_db has not been called).";
                    }
                }
                return;
            }

            foreach (var ((tableSchema, tableName), table) in tableLookup)
            {
                var captureInstances = await SqlServerCdcCatalogQueries.FetchCaptureInstancesAsync(conn, tableSchema, tableName, ct);
                if (captureInstances.Count == 0)
                {
                    table.IsCdcEnabled = false;
                    foreach (var column in table.Columns)
                    {
                        column.IsCdcCapturable = false;
                        column.UnsupportedReason = "Table is not enrolled in SQL Server CDC (sys.sp_cdc_enable_table has not been called for it).";
                    }
                    continue;
                }

                table.IsCdcEnabled = true;
                var captured = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var captureInstance in captureInstances)
                {
                    foreach (var column in await SqlServerCdcCatalogQueries.FetchCapturedColumnsAsync(conn, captureInstance, ct))
                        captured.Add(column);
                }

                foreach (var column in table.Columns)
                {
                    if (captured.Contains(column.Name))
                    {
                        column.IsCdcCapturable = true;
                    }
                    else
                    {
                        column.IsCdcCapturable = false;
                        column.UnsupportedReason = "Column is not in the CDC capture instance's column list.";
                    }
                }
            }
        }

        private static async Task<bool> IsCdcEnabledForDatabaseAsync(DbConnection conn, CancellationToken ct)
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT is_cdc_enabled FROM sys.databases WHERE database_id = DB_ID()";
            var result = await cmd.ExecuteScalarAsync(ct);
            return result is bool b ? b : Convert.ToInt32(result) != 0;
        }

        /// <summary>
        /// SQL Server JSON support is via <c>nvarchar(max)</c> with the <c>JSON</c> constraint —
        /// the column type is still string. <c>varbinary</c> / <c>image</c> are the only types
        /// that map naturally to RavenDB attachments; everything else defaults to a property.
        /// </summary>
        private static CdcColumnType SuggestType(string lowerNativeType)
        {
            return lowerNativeType switch
            {
                "varbinary" or "binary" or "image" => CdcColumnType.Attachment,
                _ => CdcColumnType.Default,
            };
        }
    }
}
