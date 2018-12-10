using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Raven.Server.SqlMigration.Schema;

namespace Raven.Server.SqlMigration.Oracle
{
    internal partial class OracleDatabaseMigrator : GenericDatabaseMigrator
    {
        public const string GetSchema = "select user_cons_columns.owner as TABLE_SCHEMA from user_cons_columns WHERE ROWNUM = 1";

        public static readonly string SelectColumns = "SELECT (select user_cons_columns.owner from user_cons_columns WHERE ROWNUM = 1) as TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM USER_TAB_COLS " +
            "where TABLE_NAME not in (select view_name from all_views where owner = (select user_cons_columns.owner from user_cons_columns WHERE ROWNUM = 1))";

        public const string SelectPrimaryKeys = "select user_cons_columns.owner as TABLE_SCHEMA, user_cons_columns.table_name, user_cons_columns.column_name " +
                                                "from user_constraints, user_cons_columns " +
                                                "where user_constraints.constraint_type = 'P' " +
                                                "and user_constraints.constraint_name = user_cons_columns.constraint_name " +
                                                "and user_constraints.owner = user_cons_columns.owner " +
                                                "order by user_cons_columns.owner, user_cons_columns.table_name, user_cons_columns.position";

        public const string SelectReferentialConstraints = "select CONSTRAINT_NAME, R_CONSTRAINT_NAME AS \"UNIQUE_CONSTRAINT_NAME\" from user_constraints where CONSTRAINT_TYPE = 'R'";

        public const string SelectKeyColumnUsage = "select user_constraints.owner as \"TABLE_SCHEMA\", user_constraints.TABLE_NAME, USER_CONS_COLUMNS.COLUMN_NAME, user_constraints.CONSTRAINT_NAME " +
                                                                      "from user_constraints inner join USER_CONS_COLUMNS on user_constraints.CONSTRAINT_NAME = USER_CONS_COLUMNS.CONSTRAINT_NAME " +
                                                                      "where user_constraints.constraint_TYPE = 'P' OR user_constraints.constraint_TYPE = 'R'";

        public override DatabaseSchema FindSchema()
        {
            using (var connection = OpenConnection())
            {
                string schemaName = null;
                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = GetSchema;
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
