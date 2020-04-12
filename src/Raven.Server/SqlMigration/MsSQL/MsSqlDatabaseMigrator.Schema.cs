using System.Collections.Generic;
using System.Data.Common;
using Raven.Server.SqlMigration.Schema;

namespace Raven.Server.SqlMigration.MsSQL
{
    internal partial class MsSqlDatabaseMigrator : GenericDatabaseMigrator
    {
        public const string SelectColumns = "SELECT C.TABLE_SCHEMA, C.TABLE_NAME, C.COLUMN_NAME, C.DATA_TYPE" +
                                            " FROM INFORMATION_SCHEMA.COLUMNS C JOIN INFORMATION_SCHEMA.TABLES T " +
                                            " ON C.TABLE_CATALOG = T.TABLE_CATALOG AND C.TABLE_SCHEMA = T.TABLE_SCHEMA AND C.TABLE_NAME = T.TABLE_NAME " +
                                            " WHERE T.TABLE_TYPE <> 'VIEW' ";

        public const string SelectPrimaryKeys = "SELECT TC.TABLE_SCHEMA, TC.TABLE_NAME, COLUMN_NAME " +
                                                "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC " +
                                                "INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU " +
                                                "ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME" +
                                                " ORDER BY ORDINAL_POSITION";

        public const string SelectReferentialConstraints = "SELECT CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME " +
                                                           "FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS";

        public const string SelectKeyColumnUsage = "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME" +
                                                                      " FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                                                                      "ORDER BY ORDINAL_POSITION";

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
