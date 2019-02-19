using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Raven.Server.SqlMigration.Schema;

namespace Raven.Server.SqlMigration.MySQL
{
    internal partial class MySqlDatabaseMigrator : GenericDatabaseMigrator
    {
        public const string SelectColumns = "select c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE " +
                                            " from information_schema.COLUMNS c join information_schema.TABLES t " +
                                            " using (TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME) " +
                                            " where c.TABLE_SCHEMA = @schema and t.TABLE_TYPE <> 'VIEW' ";

        public const string SelectPrimaryKeys = "select TABLE_NAME, COLUMN_NAME, TABLE_SCHEMA from information_schema.KEY_COLUMN_USAGE " +
                                                "where TABLE_SCHEMA = @schema and CONSTRAINT_NAME = 'PRIMARY' " +
                                                "order by ORDINAL_POSITION";

        public const string SelectReferentialConstraints = "select CONSTRAINT_SCHEMA, UNIQUE_CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_NAME, REFERENCED_TABLE_NAME " +
                                                           "from information_schema.REFERENTIAL_CONSTRAINTS " +
                                                           "where UNIQUE_CONSTRAINT_SCHEMA = @schema ";

        public const string SelectKeyColumnUsage = "select CONSTRAINT_SCHEMA, CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_COLUMN_NAME " +
                                                                      " from information_schema.KEY_COLUMN_USAGE " +
                                                                      " where TABLE_SCHEMA = @schema and CONSTRAINT_NAME <> 'PRIMARY' " +
                                                                      " order by ORDINAL_POSITION";
        
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
                DbParameter schemaParameter = cmd.CreateParameter();
                schemaParameter.ParameterName = "schema";
                schemaParameter.Value = connection.Database;
                cmd.Parameters.Add(schemaParameter);
            
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

        // Please notice it doesn't return PR for tables that doesn't reference PR using FK
        private void FindPrimaryKeys(DbConnection connection, DatabaseSchema dbSchema)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SelectPrimaryKeys;
                DbParameter schemaParameter = cmd.CreateParameter();
                schemaParameter.ParameterName = "schema";
                schemaParameter.Value = connection.Database;
                cmd.Parameters.Add(schemaParameter);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schemaAndTableName = GetTableNameFromReader(reader);
                        var tableSchema = dbSchema.GetTable(schemaAndTableName.Schema, schemaAndTableName.TableName);
                        tableSchema?.PrimaryKeyColumns.Add(reader["COLUMN_NAME"].ToString());
                    }
                }
            }
        }

        private void FindForeignKeys(DbConnection connection, DatabaseSchema dbSchema)
        {
            var keyColumnUsageCache = GetKeyColumnUsageCache(connection);
            
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SelectReferentialConstraints;
                DbParameter schemaParameter = cmd.CreateParameter();
                schemaParameter.ParameterName = "schema";
                schemaParameter.Value = connection.Database;
                cmd.Parameters.Add(schemaParameter);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var cacheKey = reader["CONSTRAINT_SCHEMA"] + ":" + reader["CONSTRAINT_NAME"];
                        var keyUsage = keyColumnUsageCache[cacheKey];

                        var referencedTableSchema = reader["UNIQUE_CONSTRAINT_SCHEMA"].ToString();
                        var referencedTableName = reader["REFERENCED_TABLE_NAME"].ToString();

                        var pkTable = dbSchema.GetTable(referencedTableSchema, referencedTableName);
                        
                        if (pkTable == null)
                        {
                            throw new InvalidOperationException("Can not find table: " + referencedTableSchema + "." + referencedTableName);
                        }
                        
                        // check if reference goes to Primary Key 
                        // note: we might have references to non-primary keys - ie. to unique index constraints 
                        if (keyUsage.ReferencedColumnNames.SequenceEqual(pkTable.PrimaryKeyColumns))
                        {
                            var tableSchema = reader["CONSTRAINT_SCHEMA"].ToString();
                            var tableName = reader["TABLE_NAME"].ToString();
                            
                            pkTable.References.Add(new TableReference(tableSchema, tableName)
                            {
                                Columns = keyUsage.ColumnNames
                            });
                        }
                    }
                }
            }
        }
        
        private Dictionary<string, (List<string> ColumnNames, List<string> ReferencedColumnNames)> GetKeyColumnUsageCache(DbConnection connection)
        {
            var cache = new Dictionary<string, (List<string> ColumnNames, List<string> ReferencedColumnNames)>();
            
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SelectKeyColumnUsage;
                DbParameter schemaParameter = cmd.CreateParameter();
                schemaParameter.ParameterName = "schema";
                schemaParameter.Value = connection.Database;

                cmd.Parameters.Add(schemaParameter);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var cacheKey = reader["CONSTRAINT_SCHEMA"] + ":" + reader["CONSTRAINT_NAME"];
                        var columnName = reader["COLUMN_NAME"].ToString();
                        var referencedColumnName = reader["REFERENCED_COLUMN_NAME"].ToString();
                        
                        if (cache.TryGetValue(cacheKey, out var cacheValue) == false)
                        {
                            cacheValue = (new List<string>(), new List<string>());
                            cache[cacheKey] = cacheValue;
                        }
                        
                        cacheValue.ColumnNames.Add(columnName);
                        cacheValue.ReferencedColumnNames.Add(referencedColumnName);
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
