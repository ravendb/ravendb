using System;
using System.Collections.Generic;
using System.Data.Common;
using Raven.Server.SqlMigration.Schema;
using Sparrow.Logging;

namespace Raven.Server.SqlMigration.NpgSQL
{
    internal partial class NpgSqlDatabaseMigrator : GenericDatabaseMigrator
    {
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
        
        
        public static Logger Logger = LoggingSource.Instance.GetLogger<NpgSqlDatabaseMigrator>("Server");
        public override DatabaseSchema FindSchema()
        {
            LogMessage("Starting FindSchema process..");
            using (var connection = OpenConnection())
            {
                var schema = new DatabaseSchema
                {
                    CatalogName = connection.Database
                };
                
                LogMessage($"Database name: {schema.CatalogName}");

                FindTableNames(connection, schema);
                FindPrimaryKeys(connection, schema);
                FindForeignKeys(connection, schema);

                return schema;
            }
        }

        private void FindTableNames(DbConnection connection, DatabaseSchema dbSchema)
        {
            LogMessage("Finding tables names...");
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = _selectColumns;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schemaAndTableName = GetTableNameFromReader(reader);
                        LogMessage($"TableName: {schemaAndTableName.TableName??"null"}, Schema: {schemaAndTableName.Schema??"null"}");

                        var tableSchema = dbSchema.GetTable(schemaAndTableName.Schema, schemaAndTableName.TableName);

                        if (tableSchema == null)
                        {
                            LogMessage("Table not in dbSchema, adding");
                            tableSchema = new SqlTableSchema(schemaAndTableName.Schema, schemaAndTableName.TableName,
                                GetSelectAllQueryForTable(schemaAndTableName.Schema, schemaAndTableName.TableName));
                            dbSchema.Tables.Add(tableSchema);
                        }
                        
                        LogMessage("Reading columns..");
                        var columnnameobj = reader["COLUMN_NAME"];
                        var columnName = columnnameobj.ToString();
                        var datatype = reader["DATA_TYPE"];
                        var columnType = MapColumnType(datatype.ToString());

                        tableSchema.Columns.Add(new TableColumn(columnType, columnName));
                        
                        LogMessage($"Added TableColumn to tableSchema.Columns... Table name: {tableSchema.TableName} \n" +
                                   $"Name: '{columnnameobj ?? "null"}', Type : '{datatype ?? "null"}'"); 
                        
                    }
                }
            }
        }

        private void FindPrimaryKeys(DbConnection connection, DatabaseSchema dbSchema)
        {
            LogMessage("Finding primary keys..");
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SelectPrimaryKeys;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schemaAndTableName = GetTableNameFromReader(reader);
                        LogMessage($"TableName: {schemaAndTableName.TableName??"null"}, Schema: {schemaAndTableName.Schema??"null"}");
                        var table = dbSchema.GetTable(schemaAndTableName.Schema, schemaAndTableName.TableName);
                        var colName = reader["COLUMN_NAME"];
                        table?.PrimaryKeyColumns.Add(colName.ToString());
                        if (table == null)
                        {
                            LogMessage($"Can't find table '{schemaAndTableName.TableName??"null"}' matching such schema in dbSchema...");
                        }
                        else
                        {
                            LogMessage($"Added column name '{colName}' to table '{schemaAndTableName.TableName??"null"}' primary key columns.");
                        }
                    }
                }
            }

        }

        private static void LogMessage(string message, Exception e = null)
        {
            if (Logger.IsOperationsEnabled)
                Logger.Operations(message, e);
        }

        private void FindForeignKeys(DbConnection connection, DatabaseSchema dbSchema)
        {
            LogMessage("Finding foreign keys..");
            var referentialConstraints = new Dictionary<string, string>();

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SelectReferentialConstraints;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var key = reader["CONSTRAINT_NAME"];
                        
                        var value = reader["UNIQUE_CONSTRAINT_NAME"];

                        LogMessage($"CONSTRAINT_NAME: {key??"null"}, UNIQUE_CONSTRAINT_NAME: {value??"null"}");
                        
                        referentialConstraints.Add(key.ToString(), value.ToString());
                    }
                }
            }


            LogMessage("Collected all referential constraints...");

            var keyColumnUsageCache = GetKeyColumnUsageCache(connection);
            
            
            foreach (var kvp in referentialConstraints)
            {
                var fkCacheValue = keyColumnUsageCache[kvp.Key];
                try
                {
                    if (keyColumnUsageCache.TryGetValue(kvp.Value, out var pkCacheValue))
                    {
                        try
                        {
                            var pkTable = dbSchema.GetTable(pkCacheValue.Schema, pkCacheValue.TableName);
                            if (pkTable == null)
                            {
                                var message = "pkTable hasn't been found for corresponding tableName and Schema\n" +
                                              $"pkCacheValue: ({pkCacheValue.Schema}, {pkCacheValue.TableName}, {String.Join('\t', pkCacheValue.ColumnNames)})";
                                LogMessage(message);
                            }

                            pkTable.References.Add(new TableReference(fkCacheValue.Schema, fkCacheValue.TableName)
                            {
                                Columns = fkCacheValue.ColumnNames
                            });
                        }
                        catch (Exception e)
                        {
                            var message = $"pkCacheValue: ({pkCacheValue.Schema}, {pkCacheValue.TableName}, {String.Join('\t', pkCacheValue.ColumnNames)})";
                            LogMessage(message, e);
                            throw;
                        }


                    }
                }
                catch (Exception e)
                {

                    var columnNames = String.Join('\t', fkCacheValue.ColumnNames);
                    var keyColumnUsageCacheKeys = String.Join(' ', keyColumnUsageCache.Keys);
                    string fkCacheValueSchema = fkCacheValue.Schema ?? "null";
                    var message = $"Failed to ... Referential constraint: cache keys - (CONSTRAINT_NAME: '{kvp.Key}', UNIQUE_CONSTRAINT_NAME: '{kvp.Value}')\n" +
                                  $"fkCacheValue: (Schema:'{fkCacheValueSchema}', TableName: '{fkCacheValue.TableName ?? "null"}', ColumnNames: '{columnNames}')\n" +
                                  $"Keys in cache: {keyColumnUsageCacheKeys}";
                    
                    if (fkCacheValue.ColumnNames.Contains(null))
                    {
                        message += ". There's a null value in the columNames!";
                    }
                    if (Logger.IsOperationsEnabled)
                    {
                        
                        Logger.Operations(message, e);
                    }
                    throw new InvalidOperationException(message, e);
                }
        
            }
               
        }

        private Dictionary<string, (string Schema, string TableName, List<string> ColumnNames)> GetKeyColumnUsageCache(DbConnection connection)
        {
            LogMessage("Creating cache for table descriptions under keys");
            var cache = new Dictionary<string, (string Schema, string TableName, List<string> ColumnNames)>();

            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = SelectKeyColumnUsage;
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var cacheKeyobj = reader["CONSTRAINT_NAME"];
                        if (cacheKeyobj == null) throw new NullReferenceException($"Cache Key is null");
                        var cacheKey = cacheKeyobj.ToString();
                        LogMessage($"Cache Key : {cacheKey}");
                        
                        (string schema, string tableName) = GetTableNameFromReader(reader);
                        if (schema == null) throw new NullReferenceException($"Schema is null, cacheKey: {cacheKey}");
                        if (tableName == null) throw new NullReferenceException($"Table name is null, cacheKey:{cacheKey}");
                        Console.WriteLine($"Schema: '{schema}', TableName: '{tableName}");
                        
                        var columnName = reader["COLUMN_NAME"].ToString();
                        if (columnName == null) throw new NullReferenceException($"Column name is null, cacheKey: {cacheKey}, tableName: {tableName}");
                        Console.WriteLine($"Column name: '{columnName}");

                        Console.WriteLine($"Cache key '{cacheKey}' already in cache: {cache.ContainsKey(cacheKey)}");
                        if (cache.TryGetValue(cacheKey, out var cacheValue) == false)
                        {
                            Console.WriteLine("Cache key not in cache, adding (schema, tableName, new List<string>()) tuple to cache...");
                            cacheValue = (schema, tableName, new List<string>());
                            cache[cacheKey] = cacheValue;
                        }

                        Console.WriteLine($"Adding column name: {columnName} to the list from the tuple...");
                        cacheValue.ColumnNames.Add(columnName);
                    }
                }
            }

            return cache;
        }

        private static (string Schema, string TableName) GetTableNameFromReader(DbDataReader reader)
        {
            var tableSchema = reader["TABLE_SCHEMA"];
            var tableName = reader["TABLE_NAME"];
            
            if (tableSchema == null)
                LogMessage($"Table schema is null for table named '{tableName??"null"}'");
            if (tableName == null)
                LogMessage($"Table name is null for table with schema '{tableSchema??"null"}'");
            
            return (tableSchema.ToString(), tableName.ToString());
        }
    }
}
