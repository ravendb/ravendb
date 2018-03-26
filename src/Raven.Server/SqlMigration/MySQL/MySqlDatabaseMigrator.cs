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

        public const string SelectPrimaryKeys = "select TABLE_NAME, COLUMN_NAME, TABLE_SCHEMA from information_schema.KEY_COLUMN_USAGE " +
                                                "where TABLE_SCHEMA = @schema and CONSTRAINT_NAME = 'PRIMARY' " +
                                                "order by ORDINAL_POSITION";

        public const string SelectReferantialConstraints = "select UNIQUE_CONSTRAINT_SCHEMA, CONSTRAINT_NAME, REFERENCED_TABLE_NAME " +
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

        protected override string QuoteTable(string schema, string tableName)
        {
            return $"`{schema}`.`{tableName}`";
        }

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

        protected override string GetQueryForCollection(RootCollection collection)
        {
            if (string.IsNullOrWhiteSpace(collection.SourceTableQuery) == false)
            {
                return collection.SourceTableQuery;
            }

            return "select * from " + QuoteTable(collection.SourceTableSchema, collection.SourceTableName);
        }

        protected override IEnumerable<SqlMigrationDocument> EnumerateTable(string tableQuery, Dictionary<string, string> documentPropertiesMapping, 
            HashSet<string> specialColumns, HashSet<string> attachmentColumns, MySqlConnection connection)
        {
            using (var cmd = new MySqlCommand(tableQuery, connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    var migrationDocument = new SqlMigrationDocument
                    {
                        Object = ExtractFromReader(reader, documentPropertiesMapping),
                        SpecialColumnsValues = ExtractFromReader(reader, specialColumns),
                        Attachments = ExtractAttachments(reader, attachmentColumns)
                    };
                    yield return migrationDocument;
                }
            }
        }

        private void FindTableNames(MySqlConnection connection, DatabaseSchema dbSchema)
        {
            using (var cmd = new MySqlCommand(SelectColumns, connection))
            {
                cmd.Parameters.AddWithValue("schema", connection.Database);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var schemaAndTableName = GetTableNameFromReader(reader);
                        var tableSchema = dbSchema.GetTable(schemaAndTableName.Schema, schemaAndTableName.TableName);
                        
                        if (tableSchema == null)
                        {
                            tableSchema = new TableSchema(schemaAndTableName.Schema, schemaAndTableName.TableName);
                            dbSchema.Tables.Add(tableSchema);
                        }

                        var columnName = reader["COLUMN_NAME"].ToString();
                        var columnType = MapColumnType(reader["DATA_TYPE"].ToString());
                        
                        tableSchema.Columns.Add(new TableColumn(columnType, columnName));
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
        private void FindPrimaryKeys(MySqlConnection connection, DatabaseSchema dbSchema)
        {
            using (var cmd = new MySqlCommand(SelectPrimaryKeys, connection))
            {
                cmd.Parameters.AddWithValue("schema", connection.Database);

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

        private void FindForeignKeys(MySqlConnection connection, DatabaseSchema dbSchema)
        {
            var referentialConstraints = new Dictionary<string, (string Schema, string Table)>();

            using (var cmd = new MySqlCommand(SelectReferantialConstraints, connection))
            {
                cmd.Parameters.AddWithValue("schema", connection.Database);

                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                        referentialConstraints.Add(reader["CONSTRAINT_NAME"].ToString(), 
                            (reader["UNIQUE_CONSTRAINT_SCHEMA"].ToString(), reader["REFERENCED_TABLE_NAME"].ToString()));
                }
            }

            foreach (var kvp in referentialConstraints)
            {
                (string Schema, string TableName) fkSchemaAndTableName = default, pkSchemaAndTableName;
                var fkColumnsName = new List<string>();

                using (var cmd = new MySqlCommand(SelectKeyColumnUsageWhereConstraintName, connection))
                {
                    cmd.Parameters.AddWithValue("constraintName", kvp.Key);
                    cmd.Parameters.AddWithValue("schema", connection.Database);

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            fkSchemaAndTableName = GetTableNameFromReader(reader);
                            fkColumnsName.Add(reader["COLUMN_NAME"].ToString());
                        }
                    }
                }

                var pkTable = dbSchema.GetTable(kvp.Value.Schema, kvp.Value.Table);

                if (pkTable == null)
                {
                    throw new InvalidOperationException("Can not find table: " + kvp.Value.Schema + "." + kvp.Value.Table);
                }

                pkTable.References.Add(new TableReference(fkSchemaAndTableName.Schema, fkSchemaAndTableName.TableName)
                {
                    Columns = fkColumnsName,
                });
            }
        }

        private static (string Schema, string TableName) GetTableNameFromReader(MySqlDataReader reader)
        {
            return (reader["TABLE_SCHEMA"].ToString(), reader["TABLE_NAME"].ToString());
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
            var query = "select * from " + QuoteTable(refInfo.SourceSchema, refInfo.SourceTableName)
                                         + " where " + string.Join(" and ", refInfo.TargetPrimaryKeyColumns.Select((column, idx) => QuoteColumn(column) + " = @p" + idx));

            return new MySqlStatementProvider<EmbeddedObjectValue>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.ForeignKeyColumns), reader =>
            {
                if (reader.Read() == false)
                {
                    // parent object is null
                    return new EmbeddedObjectValue();
                }

                return new EmbeddedObjectValue
                {
                    Object = ExtractFromReader(reader, refInfo.TargetDocumentColumns),
                    SpecialColumnsValues = ExtractFromReader(reader, refInfo.TargetSpecialColumnsNames),
                    Attachments = ExtractAttachments(reader, refInfo.TargetAttachmentColumns)
                };
            });
        }

        protected override IDataProvider<EmbeddedArrayValue> CreateArrayEmbedDataProvider(ReferenceInformation refInfo, MySqlConnection connection)
        {
            var query = "select * from " + QuoteTable(refInfo.SourceSchema, refInfo.SourceTableName)
                                         + " where " + string.Join(" and ", refInfo.ForeignKeyColumns.Select((column, idx) => QuoteColumn(column) + " = @p" + idx));

            return new MySqlStatementProvider<EmbeddedArrayValue>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.SourcePrimaryKeyColumns), reader =>
            {
                var objectProperties = new DynamicJsonArray();
                var specialProperties = new List<DynamicJsonValue>();
                var attachments = new List<Dictionary<string, byte[]>>();
                while (reader.Read())
                {
                    objectProperties.Add(ExtractFromReader(reader, refInfo.TargetDocumentColumns));
                    attachments.Add(ExtractAttachments(reader, refInfo.TargetAttachmentColumns));
                    
                    if (refInfo.ChildReferences != null)
                    {
                        // fill only when used
                        specialProperties.Add(ExtractFromReader(reader, refInfo.TargetSpecialColumnsNames));    
                        
                    }
                }

                return new EmbeddedArrayValue
                {
                    ArrayOfNestedObjects = objectProperties,
                    SpecialColumnsValues = specialProperties,
                    Attachments = attachments
                };
            });
        }

        protected override IDataProvider<DynamicJsonArray> CreateArrayLinkDataProvider(ReferenceInformation refInfo, MySqlConnection connection)
        {
            var query = "select " + string.Join(", ", refInfo.TargetPrimaryKeyColumns.Select(QuoteColumn)) + " from " + QuoteTable(refInfo.SourceSchema, refInfo.SourceTableName)
                        + " where " + string.Join(" and ", refInfo.ForeignKeyColumns.Select((column, idx) => QuoteColumn(column) + " = @p" + idx));

            return new MySqlStatementProvider<DynamicJsonArray>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.SourcePrimaryKeyColumns), reader =>
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
