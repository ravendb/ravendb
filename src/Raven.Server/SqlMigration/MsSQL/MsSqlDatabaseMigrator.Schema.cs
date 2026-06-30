using System.Collections.Generic;
using System.Data.Common;
using Raven.Server.SqlMigration.Schema;

namespace Raven.Server.SqlMigration.MsSQL
{
    internal partial class MsSqlDatabaseMigrator : GenericDatabaseMigrator
    {
        private static readonly MsSqlSchemaQueries SchemaQueries = new MsSqlSchemaQueries();

        public override DatabaseSchema FindSchema()
        {
            using (var connection = OpenConnection())
            {
                var schema = new DatabaseSchema
                {
                    CatalogName = connection.Database
                };

                FindTableNames(connection, schema);
                FindPrimaryKeys(connection, schema);
                FindForeignKeys(connection, schema);

                return schema;
            }
        }

        private void FindTableNames(DbConnection connection, DatabaseSchema dbSchema)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SchemaQueries.SelectColumnsQuery;
                SchemaQueries.AddSchemaParameter(cmd, connection);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schemaAndTableName = SqlSchemaQueries.GetTableNameFromReader(reader);

                        var tableSchema = dbSchema.GetTable(schemaAndTableName.Schema, schemaAndTableName.TableName);

                        if (tableSchema == null)
                        {
                            tableSchema = new SqlTableSchema(schemaAndTableName.Schema, schemaAndTableName.TableName,
                                GetSelectAllQueryForTable(schemaAndTableName.Schema, schemaAndTableName.TableName));
                            dbSchema.Tables.Add(tableSchema);
                        }

                        var columnName = reader["COLUMN_NAME"].ToString();
                        var columnType = MapColumnType(reader["DATA_TYPE"].ToString());

                        tableSchema.Columns.Add(new TableColumn(columnType, columnName));
                    }
                }
            }
        }

        // Please notice it doesn't return PR for tables that doesn't reference PR using FK
        private void FindPrimaryKeys(DbConnection connection, DatabaseSchema dbSchema)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SchemaQueries.SelectPrimaryKeysQuery;
                SchemaQueries.AddSchemaParameter(cmd, connection);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schemaAndTableName = SqlSchemaQueries.GetTableNameFromReader(reader);
                        var table = dbSchema.GetTable(schemaAndTableName.Schema, schemaAndTableName.TableName);
                        table?.PrimaryKeyColumns.Add(reader["COLUMN_NAME"].ToString());
                    }
                }
            }
        }

        private void FindForeignKeys(DbConnection connection, DatabaseSchema dbSchema)
        {
            var referentialConstraints = new Dictionary<string, string>();

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SchemaQueries.SelectReferentialConstraintsQuery;
                SchemaQueries.AddSchemaParameter(cmd, connection);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                        referentialConstraints.Add(reader["CONSTRAINT_NAME"].ToString(), reader["UNIQUE_CONSTRAINT_NAME"].ToString());
                }
            }

            var keyColumnUsageCache = GetKeyColumnUsageCache(connection);

            foreach (var kvp in referentialConstraints)
            {
                var fkCacheValue = keyColumnUsageCache[kvp.Key];
                if (keyColumnUsageCache.TryGetValue(kvp.Value, out var pkCacheValue))
                {
                    var pkTable = dbSchema.GetTable(pkCacheValue.Schema, pkCacheValue.TableName);

                    pkTable.References.Add(new TableReference(fkCacheValue.Schema, fkCacheValue.TableName)
                    {
                        Columns = fkCacheValue.ColumnNames
                    });
                }
            }
        }

        private Dictionary<string, (string Schema, string TableName, List<string> ColumnNames)> GetKeyColumnUsageCache(DbConnection connection)
        {
            var cache = new Dictionary<string, (string Schema, string TableName, List<string> ColumnNames)>();

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SchemaQueries.SelectKeyColumnUsageQuery;
                SchemaQueries.AddSchemaParameter(cmd, connection);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var cacheKey = reader["CONSTRAINT_NAME"].ToString();
                        (string schema, string tableName) = SqlSchemaQueries.GetTableNameFromReader(reader);
                        var columnName = reader["COLUMN_NAME"].ToString();

                        if (cache.TryGetValue(cacheKey, out var cacheValue) == false)
                        {
                            cacheValue = (schema, tableName, new List<string>());
                            cache[cacheKey] = cacheValue;
                        }

                        cacheValue.ColumnNames.Add(columnName);
                    }
                }
            }

            return cache;
        }
    }
}
