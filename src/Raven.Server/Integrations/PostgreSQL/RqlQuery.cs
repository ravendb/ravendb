using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL
{
    public class RqlQuery : PgQuery
    {
        protected readonly DocumentDatabase DocumentDatabase;
        private readonly QueryOperationContext _queryOperationContext;
        private List<Document> _result;
        private readonly int? _limit;

        public RqlQuery(string queryString, int[] parametersDataTypes, DocumentDatabase documentDatabase, int? limit = null) : base(queryString, parametersDataTypes)
        {
            DocumentDatabase = documentDatabase;

            _queryOperationContext = QueryOperationContext.Allocate(DocumentDatabase);
            _result = null;
            _limit = limit;
        }

        public override async Task<ICollection<PgColumn>> Init(bool allowMultipleStatements = false)
        {
            if (IsEmptyQuery)
                return default;

            _result = await RunRqlQuery();

            return await GenerateSchema();
        }

        public async Task<List<Document>> RunRqlQuery(string forcedQueryToRun = null)
        {
            var parameters = DynamicJsonValue.Convert(Parameters);
            var queryParameters = _queryOperationContext.Documents.ReadObject(parameters, "query/parameters");

            var indexQuery = new IndexQueryServerSide(forcedQueryToRun ?? QueryString, queryParameters);

            // If limit is 0, fetch one document for the schema generation
            if (_limit != null)
            {
                indexQuery.PageSize = _limit.Value == 0 ? 1 : _limit.Value;
            }

            var documentQueryResult =
                await DocumentDatabase.QueryRunner.ExecuteQuery(indexQuery, _queryOperationContext, null, OperationCancelToken.None);

            return documentQueryResult.Results;
        }

        protected virtual async Task<ICollection<PgColumn>> GenerateSchema()
        {
            Document sample;

            if (_result == null || _result?.Count == 0)
            {
                var query = QueryMetadata.ParseQuery(QueryString, QueryType.Select);

                query.Where = null;

                var queryWithoutFiltering = query.ToString();

                var results = await RunRqlQuery(queryWithoutFiltering);

                if (results == null || results.Count == 0)
                    return Array.Empty<PgColumn>();

                sample = results[0];
            }
            else
            {
                sample = _result[0];
            }

            var resultsFormat = GetDefaultResultsFormat();

            if (sample.Id != null)
                Columns[Constants.Documents.Indexing.Fields.DocumentIdFieldName] = new PgColumn(Constants.Documents.Indexing.Fields.DocumentIdFieldName, (short)Columns.Count, PgText.Default, resultsFormat);

            BlittableJsonReaderObject.PropertyDetails prop = default;

            // Go over sample's columns
            var properties = sample.Data.GetPropertyNames();
            for (var i = 0; i < properties.Length; i++)
            {
                // Using GetPropertyIndex to get the properties in the right order
                var propIndex = sample.Data.GetPropertyIndex(properties[i]);
                sample.Data.GetPropertyByIndex(propIndex, ref prop);

                // Skip this column, will be added later to json() column
                if (prop.Name == Constants.Documents.Metadata.Key)
                    continue;

                PgType pgType = (prop.Token & BlittableJsonReaderBase.TypesMask) switch
                {
                    BlittableJsonToken.CompressedString => PgText.Default,
                    BlittableJsonToken.String => PgText.Default,
                    BlittableJsonToken.Boolean => PgBool.Default,
                    BlittableJsonToken.EmbeddedBlittable => PgJson.Default,
                    BlittableJsonToken.Integer => PgInt8.Default,
                    BlittableJsonToken.LazyNumber => PgFloat8.Default,
                    BlittableJsonToken.Null => PgJson.Default,
                    BlittableJsonToken.StartArray => PgJson.Default,
                    BlittableJsonToken.StartObject => PgJson.Default,
                    _ => throw new NotSupportedException()
                };

                var processedString = (prop.Token & BlittableJsonReaderBase.TypesMask) switch
                {
                    BlittableJsonToken.CompressedString => (string)prop.Value,
                    BlittableJsonToken.String => (LazyStringValue)prop.Value,
                    _ => null
                };

                if (processedString != null 
                    && TypeConverter.TryConvertStringValue(processedString, out var output))
                {
                    pgType = output switch
                    {
                        DateTime dt => (dt.Kind == DateTimeKind.Utc) ? PgTimestampTz.Default : PgTimestamp.Default,
                        DateTimeOffset => PgTimestampTz.Default,
                        TimeSpan => PgInterval.Default,
                        _ => pgType
                    };
                }

                Columns.TryAdd(prop.Name, new PgColumn(prop.Name, (short)Columns.Count, pgType, resultsFormat));
            }

            if (Columns.TryGetValue(Constants.Documents.Querying.Fields.PowerBIJsonFieldName, out var jsonColumn))
            {
                jsonColumn.PgType = PgJson.Default;
            }
            else
            {
                Columns[Constants.Documents.Querying.Fields.PowerBIJsonFieldName] = new PgColumn(Constants.Documents.Querying.Fields.PowerBIJsonFieldName, (short)Columns.Count, PgJson.Default, resultsFormat);
            }

            return Columns.Values;
        }

        public static bool TryParse(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, out RqlQuery rqlQuery)
        {
            try
            {
                QueryMetadata.ParseQuery(queryText, QueryType.Select, documentDatabase);
            }
            catch
            {
                rqlQuery = null;

                return false;
            }

            rqlQuery = new RqlQuery(queryText, parametersDataTypes, documentDatabase);
            return true;
        }

        public override async Task Execute(MessageBuilder builder, PipeWriter writer, CancellationToken token)
        {
            if (IsEmptyQuery)
            {
                await writer.WriteAsync(builder.EmptyQueryResponse(), token);
                return;
            }

            if (_result == null)
                throw new InvalidOperationException("RqlQuery.Execute was called when _results = null");

            if (_limit == 0 || _result == null || _result.Count == 0)
            {
                await writer.WriteAsync(builder.CommandComplete($"SELECT 0"), token);
                return;
            }

            BlittableJsonReaderObject.PropertyDetails prop = default;
            var row = ArrayPool<ReadOnlyMemory<byte>?>.Shared.Rent(Columns.Count);

            try
            {
                short? idIndex = null;
                if (Columns.TryGetValue(Constants.Documents.Indexing.Fields.DocumentIdFieldName, out var col))
                {
                    idIndex = col.ColumnIndex;
                }

                var jsonIndex = Columns[Constants.Documents.Querying.Fields.PowerBIJsonFieldName].ColumnIndex;

                foreach (var result in _result)
                {
                    var jsonResult = result.Data;

                    Array.Clear(row, 0, row.Length);

                    if (idIndex != null && result.Id != null)
                    {
                        row[idIndex.Value] = Encoding.UTF8.GetBytes(result.Id.ToString());
                    }

                    jsonResult.Modifications = new DynamicJsonValue(jsonResult);

                    if (jsonResult.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject _))
                    {
                        // remove @metadata
                        jsonResult.Modifications.Remove(Constants.Documents.Metadata.Key);
                    }

                    foreach (var (columnName, pgColumn) in Columns)
                    {
                        var index = jsonResult.GetPropertyIndex(columnName);
                        if (index == -1)
                            continue;

                        jsonResult.GetPropertyByIndex(index, ref prop);

                        var value = GetValueByType(prop, prop.Value, pgColumn);

                        row[pgColumn.ColumnIndex] = value;

                        HandleSpecialColumnsIfNeeded(columnName, prop, prop.Value, ref row);
                        
                        jsonResult.Modifications.Remove(columnName);
                    }


                    if (jsonResult.Modifications.Removals.Count != jsonResult.Count)
                    {
                        using (DocumentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                        using (context.OpenReadTransaction())
                        {
                            var modified = context.ReadObject(jsonResult, "renew");
                            row[jsonIndex] = Encoding.UTF8.GetBytes(modified.ToString());
                        }
                    }

                    await writer.WriteAsync(builder.DataRow(row[..Columns.Count]), token);
                }
            }
            finally
            {
                ArrayPool<ReadOnlyMemory<byte>?>.Shared.Return(row);
            }

            await writer.WriteAsync(builder.CommandComplete($"SELECT {_result.Count}"), token);
        }

        protected virtual void HandleSpecialColumnsIfNeeded(string columnName, BlittableJsonReaderObject.PropertyDetails property, object value, ref ReadOnlyMemory<byte>?[] row)
        {
        }

        protected ReadOnlyMemory<byte>? GetValueByType(BlittableJsonReaderObject.PropertyDetails propertyDetails, object value, PgColumn pgColumn)
        {
            switch (propertyDetails.Token & BlittableJsonReaderBase.TypesMask, pgColumn.PgType.Oid)
            {
                case (BlittableJsonToken.Boolean, PgTypeOIDs.Bool):
                case (BlittableJsonToken.CompressedString, PgTypeOIDs.Text):
                case (BlittableJsonToken.EmbeddedBlittable, PgTypeOIDs.Json):
                case (BlittableJsonToken.Integer, PgTypeOIDs.Int8):
                case (BlittableJsonToken.String, PgTypeOIDs.Text):
                case (BlittableJsonToken.StartArray, PgTypeOIDs.Json):
                case (BlittableJsonToken.StartObject, PgTypeOIDs.Json):
                    return pgColumn.PgType.ToBytes(value, pgColumn.FormatCode);

                case (BlittableJsonToken.LazyNumber, PgTypeOIDs.Float8):
                    return pgColumn.PgType.ToBytes((double)(LazyNumberValue)value, pgColumn.FormatCode);

                case (BlittableJsonToken.CompressedString, PgTypeOIDs.Timestamp):
                case (BlittableJsonToken.CompressedString, PgTypeOIDs.TimestampTz):
                case (BlittableJsonToken.CompressedString, PgTypeOIDs.Interval):
                    {
                        if (((string)value).Length != 0 
                            && TypeConverter.TryConvertStringValue((string)value, out var obj))
                            return pgColumn.PgType.ToBytes(obj, pgColumn.FormatCode);
                        break;
                    }

                case (BlittableJsonToken.String, PgTypeOIDs.Timestamp):
                case (BlittableJsonToken.String, PgTypeOIDs.TimestampTz):
                case (BlittableJsonToken.String, PgTypeOIDs.Interval):
                    {
                        if (((LazyStringValue)value).Length != 0 
                            && TypeConverter.TryConvertStringValue((LazyStringValue)value, out object obj))
                        {
                            // Check for mismatch between column type and our data type
                            if (obj is DateTime dt)
                            {
                                if (dt.Kind == DateTimeKind.Utc 
                                    && pgColumn.PgType is not PgTimestampTz)
                                    break;

                                if (dt.Kind != DateTimeKind.Utc 
                                    && pgColumn.PgType is not PgTimestamp)
                                    break;
                            }

                            if (obj is DateTimeOffset 
                                && pgColumn.PgType is not PgTimestampTz)
                                break;

                            if (obj is TimeSpan 
                                && pgColumn.PgType is not PgInterval)
                                break;

                            return pgColumn.PgType.ToBytes(obj, pgColumn.FormatCode);
                        }
                        break;
                    }

                case (BlittableJsonToken.String, PgTypeOIDs.Float8):
                    return pgColumn.PgType.ToBytes(double.Parse((LazyStringValue)value), pgColumn.FormatCode);

                case (BlittableJsonToken.Null, PgTypeOIDs.Json):
                    return Array.Empty<byte>();
            }

            return null;
        }

        public override void Dispose()
        {
            if (IsNamedStatement)
                return;
            _queryOperationContext?.Dispose();
        }
    }
}
