using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using MySql.Data.MySqlClient;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.MsSQL;
using Raven.Server.SqlMigration.Schema;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.MySQL
{
    internal partial class MySqlDatabaseMigrator : GenericDatabaseMigrator
    {
        public const string SelectColumns = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE from information_schema.COLUMNS where TABLE_SCHEMA = @schema";

        public const string SelectPrimaryKeys = "select TABLE_NAME, COLUMN_NAME, TABLE_SCHEMA from information_schema.KEY_COLUMN_USAGE " +
                                                "where TABLE_SCHEMA = @schema and CONSTRAINT_NAME = 'PRIMARY' " +
                                                "order by ORDINAL_POSITION";

        public const string SelectReferantialConstraints = "select UNIQUE_CONSTRAINT_SCHEMA, CONSTRAINT_NAME, REFERENCED_TABLE_NAME " +
                                                           "from information_schema.REFERENTIAL_CONSTRAINTS " +
                                                           "where UNIQUE_CONSTRAINT_SCHEMA = @schema ";

        public const string SelectKeyColumnUsage = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME" +
                                                                      " from information_schema.KEY_COLUMN_USAGE " +
                                                                      "where TABLE_SCHEMA = @schema " +
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

        // Please notice it doesn't return PR for tables that doesn't referece PR using FK
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
            var referentialConstraints = new Dictionary<string, (string Schema, string Table)>();

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SelectReferantialConstraints;
                DbParameter schemaParameter = cmd.CreateParameter();
                schemaParameter.ParameterName = "schema";
                schemaParameter.Value = connection.Database;
                cmd.Parameters.Add(schemaParameter);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                        referentialConstraints.Add(reader["CONSTRAINT_NAME"].ToString(), 
                            (reader["UNIQUE_CONSTRAINT_SCHEMA"].ToString(), reader["REFERENCED_TABLE_NAME"].ToString()));
                }
            }
            
            var keyColumnUsageCache = GetKeyColumnUsageCache(connection);
            
            foreach (var kvp in referentialConstraints)
            {
                var cacheValue = keyColumnUsageCache[kvp.Key];
                var pkTable = dbSchema.GetTable(kvp.Value.Schema, kvp.Value.Table);
                
                if (pkTable == null)
                {
                    throw new InvalidOperationException("Can not find table: " + kvp.Value.Schema + "." + kvp.Value.Table);
                }
                
                pkTable.References.Add(new TableReference(cacheValue.Schema, cacheValue.TableName)
                {
                    Columns = cacheValue.ColumnNames
                });
            }
        }
        
        private Dictionary<string, (string Schema, string TableName, List<string> ColumnNames)> GetKeyColumnUsageCache(DbConnection connection)
        {
            var cache = new Dictionary<string, (string Schema, string TableName, List<string> ColumnNames)>();
            
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
