using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Raven.Server.SqlMigration.Schema;

namespace Raven.Server.SqlMigration.Oracle
{
    internal partial class OracleDatabaseMigrator : GenericDatabaseMigrator
    {
        private static readonly OracleSchemaQueries SchemaQueries = new OracleSchemaQueries();

        public override DatabaseSchema FindSchema()
        {
            using (var connection = OpenConnection())
            {
                string schemaName = null;
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = OracleSchemaQueries.GetSchemaQuery;
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            schemaName = reader["TABLE_SCHEMA"].ToString();
                        }
                    }
                }

                var schema = new DatabaseSchema
                {
                    CatalogName = schemaName ?? ConnectionString.Split(new char[] { ';' }, StringSplitOptions.RemoveEmptyEntries).First(s => s.StartsWith("USER")).Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries)[1]
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
