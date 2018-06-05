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

        public const string SelectKeyColumnUsage = "select TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME" +
                                                                      " from information_schema.KEY_COLUMN_USAGE " +
                                                                      "order by ORDINAL_POSITION";
        
        public const string ListAllDatabases = "SELECT name FROM master.dbo.sysdatabases";

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

            var keyColumnUsageCache = GetKeyColumnUsageCache(connection);
            
            foreach (var kvp in referentialConstraints)
            {
                var fkCacheValue = keyColumnUsageCache[kvp.Key];
                var pkCacheValue = keyColumnUsageCache[kvp.Value];
                
                var pkTable = dbSchema.GetTable(pkCacheValue.Schema, pkCacheValue.TableName);

                pkTable.References.Add(new TableReference(fkCacheValue.Schema, fkCacheValue.TableName)
                {
                    Columns = fkCacheValue.ColumnNames
                });
            }
        }
        
        private Dictionary<string, (string Schema, string TableName, List<string> ColumnNames)> GetKeyColumnUsageCache(SqlConnection connection)
        {
            var cache = new Dictionary<string, (string Schema, string TableName, List<string> ColumnNames)>();
            
            using (var cmd = new SqlCommand(SelectKeyColumnUsage, connection))
            {
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

            return GetSelectAllQueryForTable(collection.SourceTableSchema, collection.SourceTableName);
        }

        protected override string GetSelectAllQueryForTable(string tableSchema, string tableName)
        {
            return "select * from " + QuoteTable(tableSchema, tableName);
        }

        protected override IEnumerable<SqlMigrationDocument> EnumerateTable(string tableQuery, Dictionary<string, string> documentPropertiesMapping, 
            HashSet<string> specialColumns, Dictionary<string, string> attachmentNameMapping, SqlConnection connection, int? rowsLimit, Dictionary<string, object> queryParameters = null)
        {
            using (var cmd = new SqlCommand(LimitRowsNumber(tableQuery, rowsLimit), connection))
            {
                if (queryParameters != null)
                {
                    foreach (var kvp in queryParameters)
                    {
                        cmd.Parameters.AddWithValue(kvp.Key, kvp.Value);
                    }
                }
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var migrationDocument = new SqlMigrationDocument
                        {
                            Object = ExtractFromReader(reader, documentPropertiesMapping),
                            SpecialColumnsValues = ExtractFromReader(reader, specialColumns),
                            Attachments = ExtractAttachments(reader, attachmentNameMapping)
                        };
                        yield return migrationDocument;
                    }
                }
            }
        }
        
        private string LimitRowsNumber(string inputQuery, int? rowsLimit)
        {
            if (rowsLimit.HasValue) 
                return "select top " + rowsLimit + " rowsLimited.* from (" + inputQuery + ") rowsLimited";
            
            return inputQuery;
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

        public override List<string> GetDatabaseNames()
        {
            var dbNames = new List<string>();

            using (var connection = OpenConnection())
            using (var cmd = new SqlCommand(ListAllDatabases, connection))
            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    dbNames.Add(reader["name"].ToString());
                }
            }
            
            dbNames.Sort();

            return dbNames;
        }

        protected override string GetQueryByPrimaryKey(RootCollection collection, List<string> primaryKeyColumns, string[] primaryKeyValues, out Dictionary<string, object> queryParameters)
        {
            if (primaryKeyColumns.Count != primaryKeyValues.Length)
            {
                queryParameters = null;
                throw new InvalidOperationException("Invalid paramaters count. Primary key has " + primaryKeyColumns.Count + " columns, but " + primaryKeyValues.Length + " values were provided.");
            }
            
            var parameters = new Dictionary<string, object>();
            
            var wherePart = string.Join(" and ", primaryKeyColumns.Select((column, idx) =>
            {
                parameters["p" + idx] = primaryKeyValues[idx];
                return QuoteColumn(column) + " = @p" + idx;
            }));
            
            queryParameters = parameters;
            
            // here we ignore custom query - as we want to get row based on primary key
            return "select * from " + QuoteTable(collection.SourceTableSchema, collection.SourceTableName) + " where " + wherePart;
        }
    }
}
