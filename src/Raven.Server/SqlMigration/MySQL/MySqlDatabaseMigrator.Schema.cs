using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Raven.Server.SqlMigration.Schema;

namespace Raven.Server.SqlMigration.MySQL
{
    internal partial class MySqlDatabaseMigrator : GenericDatabaseMigrator
    {
        private static readonly MySqlSchemaQueries SchemaQueries = new MySqlSchemaQueries();

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
                cmd.CommandText = SchemaQueries.SelectReferentialConstraintsQuery;
                SchemaQueries.AddSchemaParameter(cmd, connection);

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
                cmd.CommandText = SchemaQueries.SelectKeyColumnUsageQuery;
                SchemaQueries.AddSchemaParameter(cmd, connection);

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
    }
}
