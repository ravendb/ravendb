using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.SqlMigration.Model;
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

        /// <summary>
        /// Oracle uses <c>:pN</c> parameter placeholders, but the shared
        /// <see cref="GenericDatabaseMigrator.BuildSelectByPrimaryKeyQuery"/> emits <c>@pN</c>.
        /// FetchRowsAsync was added for the CDC sink's preview endpoint, which already rejects
        /// Oracle at the handler level (<c>CdcSinkSchemaDiscovery.IsSupportedFactoryName</c>).
        /// Throw immediately here as a defensive guard so a future caller can't silently
        /// produce SQL that Oracle cannot bind. If FetchRowsAsync ever needs to run on Oracle,
        /// override <c>BuildSelectByPrimaryKeyQuery</c> / <c>BuildLimitedSelectQuery</c> with
        /// Oracle-native syntax (<c>:pN</c> placeholders and <c>FETCH NEXT n ROWS ONLY</c>)
        /// before removing this override.
        /// </summary>
        public override Task<MigratorRowFetchResult> FetchRowsAsync(
            string tableSchema,
            string tableName,
            IReadOnlyList<string> primaryKeyColumns,
            RowFetchMode mode,
            IReadOnlyList<string> primaryKeyValues,
            int maxRows,
            CancellationToken ct)
        {
            throw new NotSupportedException(
                "FetchRowsAsync is not supported on Oracle. " +
                "The shared row-fetch builder uses @pN parameter placeholders and dialect-specific " +
                "row-limit syntax that Oracle does not accept. The CDC sink (the only production " +
                "caller) already rejects Oracle at the handler level; if you need to enable this " +
                "for Oracle, override BuildSelectByPrimaryKeyQuery / BuildLimitedSelectQuery to use " +
                "Oracle-native syntax first.");
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

        protected override string BuildLimitedSelectQuery(string quotedTable, string whereClause, string orderByClause, int maxRows)
        {
            var where = string.IsNullOrEmpty(whereClause) ? string.Empty : $" where {whereClause}";
            return $"select * from {quotedTable}{where}{orderByClause} fetch next {maxRows} rows only";
        }

        protected override string GetSelectAllQueryForTable(string tableSchema, string tableName)
        {
            return $"select * from \"{tableName}\"";
        }

        protected override string GetQueryByPrimaryKey(RootCollection collection, SqlTableSchema tableSchema, string[] primaryKeyValues, out Dictionary<string, object> queryParameters)
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
