using System.Collections.Generic;
using System.Data.Common;
using Raven.Server.SqlMigration.Schema;

namespace Raven.Server.SqlMigration.NpgSQL
{
    internal partial class NpgSqlDatabaseMigrator : GenericDatabaseMigrator
    {
        public const string SelectColumns = "select c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE" +
                                            " from information_schema.COLUMNS c join information_schema.TABLES t " +
                                            " on c.TABLE_CATALOG = t.TABLE_CATALOG and c.TABLE_SCHEMA = t.TABLE_SCHEMA and c.TABLE_NAME = t.TABLE_NAME " +
                                            " where t.TABLE_TYPE <> 'VIEW' AND t.table_schema = 'public'";

        public const string SelectPrimaryKeys = "select tc.TABLE_SCHEMA, tc.TABLE_NAME, COLUMN_NAME " +
                                                "from information_schema.TABLE_CONSTRAINTS as tc " +
                                                "inner join information_schema.KEY_COLUMN_USAGE as ku " +
                                                "on tc.CONSTRAINT_TYPE = 'PRIMARY KEY' and tc.constraint_name = ku.CONSTRAINT_NAME" +
                                                " order by ORDINAL_POSITION";

        public const string SelectReferentialConstraints = "select CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME " +
                                                           "from information_schema.REFERENTIAL_CONSTRAINTS";

        public const string SelectKeyColumnUsage = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME" +
                                                                      " from information_schema.KEY_COLUMN_USAGE " +
                                                                      "order by ORDINAL_POSITION";

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
                cmd.CommandText = SelectColumns;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schemaAndTableName = GetTableNameFromReader(reader);

                        var tableSchema = dbSchema.GetTable(schemaAndTableName.Schema, schemaAndTableName.TableName);

                        if (tableSchema == null)
                        {
                            tableSchema = new TableSchema(schemaAndTableName.Schema, schemaAndTableName.TableName,
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

        private void FindPrimaryKeys(DbConnection connection, DatabaseSchema dbSchema)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SelectPrimaryKeys;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schemaAndTableName = GetTableNameFromReader(reader);
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
                cmd.CommandText = SelectReferentialConstraints;
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
                cmd.CommandText = SelectKeyColumnUsage;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var cacheKey = reader["CONSTRAINT_NAME"].ToString();
                        (string schema, string tableName) = GetTableNameFromReader(reader);
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

        private static (string Schema, string TableName) GetTableNameFromReader(DbDataReader reader)
        {
            return (reader["TABLE_SCHEMA"].ToString(), reader["TABLE_NAME"].ToString());
        }
    }
}
