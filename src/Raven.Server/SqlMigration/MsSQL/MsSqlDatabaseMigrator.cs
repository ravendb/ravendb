using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.Schema;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.MsSQL
{
    internal class MsSqlDatabaseMigrator : GenericDatabaseMigrator<SqlConnection>
    {
        public const string SelectColumns = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE from information_schema.COLUMNS";

        public const string SelectPrimaryKeys = "select tc.TABLE_SCHEMA, tc.TABLE_NAME, COLUMN_NAME " +
                                                "from information_schema.TABLE_CONSTRAINTS as tc " +
                                                "inner join information_schema.KEY_COLUMN_USAGE as ku " +
                                                "on tc.CONSTRAINT_TYPE = 'PRIMARY KEY' and tc.constraint_name = ku.CONSTRAINT_NAME" +
                                                " order by ORDINAL_POSITION";

        public const string SelectReferantialConstraints = "select CONSTRAINT_NAME, UNIQUE_CONSTRAINT_NAME " +
                                                           "from information_schema.REFERENTIAL_CONSTRAINTS";

        public const string SelectKeyColumnUsageWhereConstraintName = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME" +
                                                                      " from information_schema.KEY_COLUMN_USAGE " +
                                                                      "where CONSTRAINT_NAME = @constraintName " +
                                                                      "order by ORDINAL_POSITION";

        private readonly string _connectionString;

        public MsSqlDatabaseMigrator(string connectionString)
        {
            _connectionString = connectionString;
        }

        protected override string QuoteColumn(string columnName)
        {
            return $"[{columnName}]";
        }

        protected override string QuoteTable(string schema, string tableName)
        {
            return $"[{schema}].[{tableName}]";
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

        private void FindTableNames(SqlConnection connection, DatabaseSchema dbSchema)
        {
            using (var cmd = new SqlCommand(SelectColumns, connection))
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

        private ColumnType MapColumnType(string type)
        {
            type = type.ToLower();

            switch (type)
            {
                case "bigint":
                case "decimal":
                case "float":
                case "int":
                case "smallint":
                case "tinyint":
                    return ColumnType.Number;
                case "bit":
                    return ColumnType.Boolean;
                case "binary":
                case "varbinary":
                    return ColumnType.Binary;
                case "char":
                case "nchar":
                case "nvarchar":
                case "datetime":
                case "datetime2":
                case "datetimeoffset":
                case "time":
                case "timestamp":
                case "uniqueidentifier":
                case "varchar":
                    return ColumnType.String;

                default:
                    return ColumnType.Unsupported;
            }
        }

        // Please notice it doesn't return PR for tables that doesn't referece PR using FK
        private void FindPrimaryKeys(SqlConnection connection, DatabaseSchema dbSchema)
        {
            using (var cmd = new SqlCommand(SelectPrimaryKeys, connection))
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

        private void FindForeignKeys(SqlConnection connection, DatabaseSchema dbSchema)
        {
            var referentialConstraints = new Dictionary<string, string>();

            using (var cmd = new SqlCommand(SelectReferantialConstraints, connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                    referentialConstraints.Add(reader["CONSTRAINT_NAME"].ToString(), reader["UNIQUE_CONSTRAINT_NAME"].ToString());
            }

            foreach (var kvp in referentialConstraints)
            {
                (string Schema, string TableName) fkSchemaAndTableName = default, pkSchemaAndTableName;
                var fkColumnsName = new List<string>();

                using (var cmd = new SqlCommand(SelectKeyColumnUsageWhereConstraintName, connection))
                {
                    cmd.Parameters.AddWithValue("constraintName", kvp.Key);

                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            fkSchemaAndTableName = GetTableNameFromReader(reader);
                            fkColumnsName.Add(reader["COLUMN_NAME"].ToString());
                        }
                    }
                }

                using (var cmd = new SqlCommand(SelectKeyColumnUsageWhereConstraintName, connection))
                {
                    cmd.Parameters.AddWithValue("constraintName", kvp.Value);

                    using (var reader = cmd.ExecuteReader())
                    {
                        if (reader.Read())
                        {
                            pkSchemaAndTableName = GetTableNameFromReader(reader);
                        }
                        else
                        {
                            throw new InvalidOperationException("Expected at least one record when when searching for foreign key relationship: " + kvp.Key + " -> " +
                                                                kvp.Value);
                        }
                    }
                }

                var pkTable = dbSchema.GetTable(pkSchemaAndTableName.Schema, pkSchemaAndTableName.TableName);

                pkTable.References.Add(new TableReference(fkSchemaAndTableName.Schema, fkSchemaAndTableName.TableName)
                {
                    Columns = fkColumnsName
                });
            }
        }

        private static (string Schema, string TableName) GetTableNameFromReader(SqlDataReader reader)
        {
            return (reader["TABLE_SCHEMA"].ToString(), reader["TABLE_NAME"].ToString());
        }

        protected override SqlConnection OpenConnection()
        {
            SqlConnection connection;

            try
            {
                connection = new SqlConnection(_connectionString);
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

        protected override string GetQueryForCollection(RootCollection collection)
        {
            if (string.IsNullOrWhiteSpace(collection.SourceTableQuery) == false)
            {
                return collection.SourceTableQuery;
            }

            return "select * from " + QuoteTable(collection.SourceTableSchema, collection.SourceTableName);
        }

        protected override IEnumerable<SqlMigrationDocument> EnumerateTable(string tableQuery, Dictionary<string, string> documentPropertiesMapping, 
            HashSet<string> specialColumns, HashSet<string> attachmentColumns, SqlConnection connection)
        {
            using (var cmd = new SqlCommand(tableQuery, connection))
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

        protected override IDataProvider<DynamicJsonArray> CreateArrayLinkDataProvider(ReferenceInformation refInfo, SqlConnection connection)
        {
            var query = "select " + string.Join(", ", refInfo.TargetPrimaryKeyColumns.Select(QuoteColumn)) + " from " + QuoteTable(refInfo.SourceSchema, refInfo.SourceTableName)
                        + " where " + string.Join(" and ", refInfo.ForeignKeyColumns.Select((column, idx) => QuoteColumn(column) + " = @p" + idx));


            return new MsSqlStatementProvider<DynamicJsonArray>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.SourcePrimaryKeyColumns), reader =>
            {
                var result = new DynamicJsonArray();
                while (reader.Read())
                {
                    var linkParameters = new object[reader.FieldCount];
                    for (var i = 0; i < linkParameters.Length; i++)
                    {
                        linkParameters[i] = reader[i];
                    }

                    result.Add(GenerateDocumentId(refInfo.CollectionNameToUseInLinks, linkParameters));
                }

                return result;
            });
        }

        protected override IDataProvider<EmbeddedObjectValue> CreateObjectEmbedDataProvider(ReferenceInformation refInfo, SqlConnection connection)
        {
            var query = "select * from " + QuoteTable(refInfo.SourceSchema, refInfo.SourceTableName)
                                         + " where " + string.Join(" and ", refInfo.TargetPrimaryKeyColumns.Select((column, idx) => QuoteColumn(column) + " = @p" + idx));

            return new MsSqlStatementProvider<EmbeddedObjectValue>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.ForeignKeyColumns), reader =>
            {
                if (reader.Read() == false)
                {
                    // parent object is null
                    return new EmbeddedObjectValue();
                }

                return new EmbeddedObjectValue {
                    Object = ExtractFromReader(reader, refInfo.TargetDocumentColumns),
                    SpecialColumnsValues = ExtractFromReader(reader, refInfo.TargetSpecialColumnsNames),
                    Attachments = ExtractAttachments(reader, refInfo.TargetAttachmentColumns)
                };
            });
        }

        protected override IDataProvider<EmbeddedArrayValue> CreateArrayEmbedDataProvider(ReferenceInformation refInfo, SqlConnection connection)
        {
            var query = "select * from " + QuoteTable(refInfo.SourceSchema, refInfo.SourceTableName)
                                         + " where " + string.Join(" and ", refInfo.ForeignKeyColumns.Select((column, idx) => QuoteColumn(column) + " = @p" + idx));

            return new MsSqlStatementProvider<EmbeddedArrayValue>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.SourcePrimaryKeyColumns), reader =>
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
    }
}
