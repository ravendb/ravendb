using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Raven.Server.SqlMigration.Model;
using Raven.Server.SqlMigration.MsSQL;
using Raven.Server.SqlMigration.Schema;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Oracle
{
    internal partial class OracleDatabaseMigrator : GenericDatabaseMigrator
    {
        protected override string FactoryName => "Oracle.ManagedDataAccess.Client";

        public OracleDatabaseMigrator(string connectionString) : base(connectionString)
        {   
        }

        protected override string QuoteColumn(string columnName)
        {
            return $"{columnName}";
        }

        protected override string QuoteTable(string schema, string tableName)
        {
            return $"{tableName}";
        }
        
        private ColumnType MapColumnType(string type)
        {
            type = type.ToLower();



            switch (type)
            {
                // text
                case "char":
                case "nchar":
                case "nvarchar2":
                case "varchar2":
                case "varchar":
                case "long":
                case "raw":
                case "long raw":
                // date
                case "date":
                case "timestamp":
                case "timestamp with time zone":
                case "timestamp with local time zone":
                case "interval year to month":
                case "interval day to second":
                case "timestamp(6)":
                case "timestamp(0)":
                    return ColumnType.String;

                case "smallint":
                case "integer":
                case "int":
                case "real":
                case "double precision":
                case "decimal":
                case "dec":
                case "float":
                case "numeric":
                case "number":
                case "BINARY_FLOAT":
                case "BINARY_DOUBLE":
                case "YEAR":                
                case "MONTH":              
                case "HOUR":                
                case "MINUTE":             
                case "SECOND":             
                    return ColumnType.Number;

                case "bfile":
                case "blob":
                case "clob":
                case "nclob":
                    return ColumnType.Binary;

                case "array":
                    return ColumnType.Array;
                default:
                    return ColumnType.Unsupported;
            }
        }
        
        protected override string LimitRowsNumber(string inputQuery, int? rowsLimit)
        {
            if (rowsLimit.HasValue)
            {
                return $"select * from ({inputQuery}) FETCH NEXT {rowsLimit} ROWS ONLY";
            }


            return inputQuery;
        }

        protected override string GetSelectAllQueryForTable(string tableSchema, string tableName)
        {
            return $"select * from \"{tableName}\"";
        }

        protected override string GetQueryByPrimaryKey(RootCollection collection, TableSchema tableSchema, string[] primaryKeyValues, out Dictionary<string, object> queryParameters)
        {
            var primaryKeyColumns = tableSchema.PrimaryKeyColumns;
            if (primaryKeyColumns.Count != primaryKeyValues.Length)
            {
                queryParameters = null;
                throw new InvalidOperationException("Invalid parameters count. Primary key has " + primaryKeyColumns.Count + " columns, but " + primaryKeyValues.Length + " values were provided.");
            }

            var parameters = new Dictionary<string, object>();
            string query = $"select * from \"{QuoteTable(collection.SourceTableSchema, collection.SourceTableName)}\" where "
                           + string.Join(" and ", primaryKeyColumns.Select((column, idx) =>
                           {
                               parameters["p" + idx] = ValueAsObject(tableSchema, column, primaryKeyValues, idx);
                               return $"\"{QuoteColumn(column)}\" = :p{idx}";
                           }));


            queryParameters = parameters;
            return query;
        }

        protected override IDataProvider<DynamicJsonArray> CreateArrayLinkDataProvider(ReferenceInformation refInfo, DbConnection connection)
        {
            var queryColumns = string.Join(", ", refInfo.TargetPrimaryKeyColumns.Select(QuoteColumn));
            var queryParameters = string.Join(" and ", refInfo.ForeignKeyColumns.Select((column, idx) => $"\"{QuoteColumn(column)}\" = :p{idx}"));

            var query = $"select \"{queryColumns}\" from \"{QuoteTable(refInfo.SourceSchema, refInfo.SourceTableName)}\" where {queryParameters}";

            return new SqlStatementProvider<DynamicJsonArray>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.SourcePrimaryKeyColumns), reader =>
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

        protected override IDataProvider<EmbeddedObjectValue> CreateObjectEmbedDataProvider(ReferenceInformation refInfo, DbConnection connection)
        {
            var queryParameters = string.Join(" and ", refInfo.TargetPrimaryKeyColumns.Select((column, idx) => $"\"{QuoteColumn(column)}\" = :p{idx}"));
            var query = $"select * from \"{QuoteTable(refInfo.SourceSchema, refInfo.SourceTableName)}\" where {queryParameters}";
            
            return new SqlStatementProvider<EmbeddedObjectValue>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.ForeignKeyColumns), reader =>
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

        protected override IDataProvider<EmbeddedArrayValue> CreateArrayEmbedDataProvider(ReferenceInformation refInfo, DbConnection connection)
        {
            var queryParameters = string.Join(" and ", refInfo.ForeignKeyColumns.Select((column, idx) => $"\"{QuoteColumn(column)}\" = :p{idx}"));

            var query = $"select * from \"{QuoteTable(refInfo.SourceSchema, refInfo.SourceTableName)}\" where {queryParameters}";

            return new SqlStatementProvider<EmbeddedArrayValue>(connection, query, specialColumns => GetColumns(specialColumns, refInfo.SourcePrimaryKeyColumns), reader =>
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
