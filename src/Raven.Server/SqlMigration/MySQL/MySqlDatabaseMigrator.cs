using System;
using System.Collections.Generic;
using System.Linq;
using MySql.Data.MySqlClient;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.Schema;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.MySQL
{
    internal class MySqlDatabaseMigrator : GenericDatabaseMigrator<MySqlConnection>
    {
        public const string SelectColumns = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE from information_schema.COLUMNS where TABLE_SCHEMA = @schema";

        public const string SelectPrimaryKeys = "select TABLE_NAME, COLUMN_NAME from information_schema.KEY_COLUMN_USAGE " +
                                                "where TABLE_SCHEMA = @schema and CONSTRAINT_NAME = 'PRIMARY' " +
                                                "order by ORDINAL_POSITION";

        public const string SelectReferantialConstraints = "select CONSTRAINT_NAME, REFERENCED_TABLE_NAME " +
                                                           "from information_schema.REFERENTIAL_CONSTRAINTS " +
                                                           "where UNIQUE_CONSTRAINT_SCHEMA = @schema ";

        public const string SelectKeyColumnUsageWhereConstraintName = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME" +
                                                                      " from information_schema.KEY_COLUMN_USAGE " +
                                                                      "where CONSTRAINT_NAME = @constraintName and TABLE_SCHEMA = @schema " +
                                                                      "order by ORDINAL_POSITION";

        private readonly string _connectionString;

        public MySqlDatabaseMigrator(string connectionString)
        {
            _connectionString = connectionString;
        }

        protected override string QuoteColumn(string columnName)
        {
            return $"`{columnName}`";
        }

        protected override string QuoteTable(string tableName)
        {
            return $"`{tableName}`";
        }

        public override DatabaseSchema FindSchema()
        {
            using (var connection = OpenConnection())
            {
                var schema = new DatabaseSchema
                {
                    Name = connection.Database
                };

                FindTableNames(connection, schema.Tables);
                FindPrimaryKeys(connection, schema.Tables);
                FindForeignKeys(connection, schema.Tables);

                return schema;
            }
        }

        protected override string GetQueryForCollection(RootCollection collection)
        {
            if (string.IsNullOrWhiteSpace(collection.SourceTableQuery) == false)
            {
                return collection.SourceTableQuery;
            }

            return "select * from " + QuoteTable(collection.SourceTableName);
        }

        protected override IEnumerable<SqlMigrationDocument> EnumerateTable(string tableQuery, HashSet<string> specialColumns, MySqlConnection connection)
        {
            using (var cmd = new MySqlCommand(tableQuery, connection))
            using (var reader = cmd.ExecuteReader())
            {
                var columnNames = new List<string>();
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    var columnName = reader.GetName(i);
                    if (specialColumns.Contains(columnName) == false)
                    {
                        columnNames.Add(columnName);
                    }
                }

                while (reader.Read())
                {
                    var migrationDocument = new SqlMigrationDocument(null);
                    migrationDocument.Object = ExtractFromReader(reader, columnNames);
                    migrationDocument.SpecialColumnsValues = ExtractFromReader(reader, specialColumns);
                    yield return migrationDocument;
                }
            }
        }

        private void FindTableNames(MySqlConnection connection, Dictionary<string, TableSchema> tables)
        {
            using (var cmd = new MySqlCommand(SelectColumns, connection))
            {
                cmd.Parameters.AddWithValue("schema", connection.Database);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = GetTableNameFromReader(reader);

                        if (tables.TryGetValue(tableName, out var tableSchema) == false)
                        {
                            tableSchema = new TableSchema();
                            tables[tableName] = tableSchema;
                        }

                        tableSchema.Columns.Add(new TableColumn
                        {
                            Name = reader["COLUMN_NAME"].ToString(),
                            Type = MapColumnType(reader["DATA_TYPE"].ToString())
                        });
                    }
                }
            }
        }

        private ColumnType MapColumnType(string type)
        {
            type = type.ToLower();

            switch (type)
            {
                case "varchar":
                case "longtext":

                case "enum":
                case "text":
                case "timestamp":
                case "char":
                case "set":
                case "mediumtext":
                case "time":
                case "date":
                case "datetime":
                case "tinytext":
                    return ColumnType.String;

                case "bigint":
                case "int":
                case "tinyint":
                case "decimal":
                case "double":
                case "float":
                case "real":
                case "year":
                case "smallint":
                case "mediumint":
                    return ColumnType.Number;

                case "bit":
                    return ColumnType.Boolean;

                case "tinyblob":
                case "blob":
                case "mediumblob":
                case "longblob":
                case "binary":
                case "varbinary":
                    return ColumnType.Binary;

                default:
                    return ColumnType.Unsupported;
            }
        }

        // Please notice it doesn't return PR for tables that doesn't referece PR using FK
        private void FindPrimaryKeys(MySqlConnection connection, Dictionary<string, TableSchema> tables)
        {
            using (var cmd = new MySqlCommand(SelectPrimaryKeys, connection))
            {
                cmd.Parameters.AddWithValue("schema", connection.Database);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var tableName = GetTableNameFromReader(reader);
                        if (tables.TryGetValue(tableName, out var tableSchema))
                        {
                            tableSchema.PrimaryKeyColumns.Add(reader["COLUMN_NAME"].ToString());
                        }
                    }
                }
            }
        }

        private void FindForeignKeys(MySqlConnection connection, Dictionary<string, TableSchema> tables)
        {
            var referentialConstraints = new Dictionary<string, string>();

            using (var cmd = new MySqlCommand(SelectReferantialConstraints, connection))
            {
                cmd.Parameters.AddWithValue("schema", connection.Database);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                        referentialConstraints.Add(reader["CONSTRAINT_NAME"].ToString(), reader["REFERENCED_TABLE_NAME"].ToString());
                }
            }

            foreach (var kvp in referentialConstraints)
            {
                string fkTableName = null, pkTableName = null;
                var fkColumnsName = new List<string>();

                using (var cmd = new MySqlCommand(SelectKeyColumnUsageWhereConstraintName, connection))
                {
                    cmd.Parameters.AddWithValue("constraintName", kvp.Key);
                    cmd.Parameters.AddWithValue("schema", connection.Database);

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            fkTableName = GetTableNameFromReader(reader);
                            fkColumnsName.Add(reader["COLUMN_NAME"].ToString());
                        }
                    }
                }

                var pkTable = tables
                    .SingleOrDefault(x => x.Key == kvp.Value);

                if (pkTable.Value == null)
                {
                    throw new InvalidOperationException("Can not find table: " + kvp.Value);
                }

                pkTable.Value.References.Add(new TableReference
                {
                    Columns = fkColumnsName,
                    Table = fkTableName
                });
            }
        }

        private static string GetTableNameFromReader(MySqlDataReader reader)
        {
            return reader["TABLE_NAME"].ToString();
        }

        protected override MySqlConnection OpenConnection()
        {
            MySqlConnection connection;

            try
            {
                connection = new MySqlConnection(_connectionString);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Cannot create new sql connection using the given connection string", e);
            }

            try
            {
                connection.Open();
            }
            catch (Exception e)
            {
                throw new InvalidOperationException("Cannot open connection using the given connection string", e);
            }

            return connection;
        }

        protected override IDataProvider<EmbeddedObjectValue> CreateObjectEmbedDataProvider(ReferenceInformation refInfo, MySqlConnection connection)
        {
            var query = "select * from " + QuoteTable(refInfo.SourceTableName)
                                         + " where " + string.Join(" and ", refInfo.TargetPrimaryKeyColumns.Select((column, idx) => QuoteColumn(column) + " = @p" + idx));

            return new MySqlPreparedStatementProvider<EmbeddedObjectValue>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.ForeignKeyColumns), reader =>
            {
                if (reader.Read() == false)
                {
                    throw new InvalidOperationException("Excepted at least single result."); //TODO: better exception
                }

                return new EmbeddedObjectValue
                {
                    Object = ExtractFromReader(reader, refInfo.TargetDocumentColumns),
                    SpecialColumnsValues = ExtractFromReader(reader, refInfo.TargetSpecialColumnsNames)
                };
            });
        }

        protected override IDataProvider<EmbeddedArrayValue> CreateArrayEmbedDataProvider(ReferenceInformation refInfo, MySqlConnection connection)
        {
            var query = "select * from " + QuoteTable(refInfo.SourceTableName)
                                         + " where " + string.Join(" and ", refInfo.ForeignKeyColumns.Select((column, idx) => QuoteColumn(column) + " = @p" + idx));

            return new MySqlPreparedStatementProvider<EmbeddedArrayValue>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.SourcePrimaryKeyColumns), reader =>
            {
                var objectProperties = new DynamicJsonArray();
                var specialProperties = new List<DynamicJsonValue>();
                while (reader.Read())
                {
                    objectProperties.Add(ExtractFromReader(reader, refInfo.TargetDocumentColumns));
                    
                    if (refInfo.ChildReferences != null)
                    {
                        // fill only when used
                        specialProperties.Add(ExtractFromReader(reader, refInfo.TargetSpecialColumnsNames));                        
                    }
                }

                return new EmbeddedArrayValue
                {
                    ArrayOfNestedObjects = objectProperties,
                    SpecialColumnsValues = specialProperties
                };
            });
        }

        protected override IDataProvider<DynamicJsonArray> CreateArrayLinkDataProvider(ReferenceInformation refInfo, MySqlConnection connection)
        {
            var query = "select " + string.Join(", ", refInfo.TargetPrimaryKeyColumns.Select(QuoteColumn)) + " from " + QuoteTable(refInfo.SourceTableName)
                        + " where " + string.Join(" and ", refInfo.ForeignKeyColumns.Select((column, idx) => QuoteColumn(column) + " = @p" + idx));

            return new MySqlPreparedStatementProvider<DynamicJsonArray>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.SourcePrimaryKeyColumns), reader =>
            {
                var result = new DynamicJsonArray();
                while (reader.Read())
                {
                    var linkParameters = new object[reader.FieldCount];
                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        linkParameters[i] = reader[i];
                    }

                    result.Add(GenerateDocumentId(refInfo.CollectionNameToUseInLinks, linkParameters));
                }

                return result;
            });
        }
    }
}
