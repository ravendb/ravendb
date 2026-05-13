using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
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
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Integrations.PostgreSQL
{
    public class RqlQuery : PgQuery
    {
        protected readonly DocumentDatabase DocumentDatabase;
        private QueryOperationContext _queryOperationContext;
        private List<Document> _samples;
        private readonly int? _limit;
        private bool _queryWasRun;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<PgSession>("Postgres RqlQuery");

        private const int SchemaInferenceSampleSize = 1024;

        ~RqlQuery()
        {
            try
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Query '{QueryString ?? "null"}' wasn't disposed properly.{Environment.NewLine}" +
                                      $"Query was run: {_queryWasRun}{Environment.NewLine}" +
                                      $"Are transactions still opened: {_queryOperationContext?.AreTransactionsOpened() ?? false}{Environment.NewLine}");
                Dispose();
            }
            catch (Exception)
            {
                // ignored - making sure we won't throw from finalizer crashing the process
            }
        }

        public RqlQuery(string queryString, int[] parametersDataTypes, DocumentDatabase documentDatabase, int? limit = null) : base(queryString, parametersDataTypes)
        {
            DocumentDatabase = documentDatabase;
            _limit = limit;
        }

        public override async Task<ICollection<PgColumn>> Init(bool allowMultipleStatements = false)
        {
            if (IsEmptyQuery)
                return default;

            _samples = await RunRqlQuery();
            return await GenerateSchema();
        }

        private async Task<List<Document>> RunRqlQuery(string forcedQueryToRun = null)
        {
            _queryOperationContext ??= QueryOperationContext.Allocate(DocumentDatabase);
            var parameters = DynamicJsonValue.Convert(Parameters);
            var queryParameters = _queryOperationContext.Documents.ReadObject(parameters, "query/parameters");

            var indexQuery = new IndexQueryServerSide(forcedQueryToRun ?? QueryString, queryParameters);

            // if limit is 0, fetch one document for schema generation
            indexQuery.PageSize = _limit == 0 ? 1 : SchemaInferenceSampleSize;

            using var cancelToken = new OperationCancelToken(DocumentDatabase.DatabaseShutdown);

            _queryWasRun = true;
            var documentQueryResult =
                await DocumentDatabase.QueryRunner.ExecuteQuery(indexQuery, _queryOperationContext, null, cancelToken);

            return documentQueryResult.Results;
        }

        protected virtual async Task<ICollection<PgColumn>> GenerateSchema()
        {
            if (_samples == null || _samples.Count == 0)
            {
                var query = QueryMetadata.ParseQuery(QueryString, QueryType.Select);

                query.Where = null;

                var queryWithoutFiltering = query.ToString();

                _samples = await RunRqlQuery(queryWithoutFiltering);

                if (_samples == null || _samples.Count == 0)
                    return Array.Empty<PgColumn>();
            }

            var resultsFormat = GetDefaultResultsFormat();

            if (_samples[0].Id != null)
                Columns[Constants.Documents.Indexing.Fields.DocumentIdFieldName] = new PgColumn(Constants.Documents.Indexing.Fields.DocumentIdFieldName, (short)Columns.Count, PgText.Default, resultsFormat);

            BlittableJsonReaderObject.PropertyDetails prop = default;
            
            // If there's a null value in a particular column of the record, don't write null type to the schema.
            // Instead, iterate over results trying to find a record with the value filled in this column.
            // Keep 'unchecked type' columns names in the list below. 
            var uncheckedTypePropertiesNames = _samples[0].Data.GetPropertyNames().ToList();
            
            // Skip metadata() column, so it will be added later to json() column
            uncheckedTypePropertiesNames.Remove(Constants.Documents.Metadata.Key);
            
            // Fulfill the 'Columns' to prevent losing the order later.
            // Assign them null type (PgJson.Default) at the start.
            foreach (var property in uncheckedTypePropertiesNames.ToArray())
                Columns.TryAdd(property, new PgColumn(property, (short)Columns.Count, PgJson.Default, resultsFormat));
            
            // Go through results - we'll try to find all properties types.
            for (int sampleIndex = 0; sampleIndex < _samples.Count && sampleIndex < 1000; sampleIndex++)
            {
                Document sample = _samples[sampleIndex];
                
                // Iterate over the columns which type hasn't been figured out yet
                var uncheckedTypePropertiesNamesCopy = uncheckedTypePropertiesNames.ToArray();
                foreach (var propertyName in uncheckedTypePropertiesNamesCopy)
                {
                    // Using GetPropertyIndex to get the properties in the right order
                    var propIndex = sample.Data.GetPropertyIndex(propertyName);

                    // If the document does not have this property, there is nothing to do.
                    if (propIndex == -1)
                        continue;
                    
                    sample.Data.GetPropertyByIndex(propIndex, ref prop);
                    
                    if (prop.Value == null)
                        continue;  // nothing to do here.
                
                    var bjt = prop.Token & BlittableJsonReaderBase.TypesMask;
                    PgType pgType = bjt switch
                    {
                        BlittableJsonToken.CompressedString => PgText.Default,
                        BlittableJsonToken.String => PgText.Default,
                        BlittableJsonToken.Boolean => PgBool.Default,
                        BlittableJsonToken.EmbeddedBlittable => PgJson.Default,
                        BlittableJsonToken.Integer => PgInt8.Default,
                        BlittableJsonToken.LazyNumber => PgFloat8.Default,
                        BlittableJsonToken.Null => PgJson.Default, // it should never hit that case by design
                        BlittableJsonToken.StartArray => PgJson.Default,
                        BlittableJsonToken.StartObject => PgJson.Default,
                        _ => throw new NotSupportedException()
                    };

                    var processedString = (prop.Token & BlittableJsonReaderBase.TypesMask) switch
                    {
                        BlittableJsonToken.CompressedString => (string)(LazyCompressedStringValue)prop.Value,
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

                    uncheckedTypePropertiesNames.Remove(propertyName);
                    Columns[propertyName] = new PgColumn(propertyName, Columns[propertyName].ColumnIndex, pgType, resultsFormat);
                }

                // If we're finished, break
                if (uncheckedTypePropertiesNames.Count == 0)
                    break;
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
                QueryMetadata.ParseQuery(queryText, QueryType.Select);
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
            try
            {
                if (IsEmptyQuery)
                {
                    await writer.WriteAsync(builder.EmptyQueryResponse(), token);
                    return;
                }

                if (_limit == 0 || _samples == null || _samples.Count == 0)
                {
                    await writer.WriteAsync(builder.CommandComplete($"SELECT 0"), token);
                    return;
                }

                _queryOperationContext ??= QueryOperationContext.Allocate(DocumentDatabase);

                var parameters = DynamicJsonValue.Convert(Parameters);
                var queryParameters = _queryOperationContext.Documents.ReadObject(parameters, "query/parameters");
                var indexQuery = new IndexQueryServerSide(QueryString, queryParameters);

                if (_limit.HasValue)
                    indexQuery.PageSize = _limit.Value;

                await using var streamWriter = new PgStreamDocumentQueryResultWriter(writer, builder, Columns, DocumentDatabase);

                using var cancelToken = new OperationCancelToken(DocumentDatabase.DatabaseShutdown, token);
                await DocumentDatabase.QueryRunner.ExecuteStreamQuery(indexQuery, _queryOperationContext, NopHttpResponse.Instance, streamWriter, cancelToken);

                await writer.WriteAsync(builder.CommandComplete($"SELECT {streamWriter.Count}"), token);
            }
            finally
            {
                ReleaseQueryResources();
            }
            
        }

        protected virtual void HandleSpecialColumnsIfNeeded(string columnName, BlittableJsonReaderObject.PropertyDetails property, object value, ref ReadOnlyMemory<byte>?[] row)
        {
        }

        internal static ReadOnlyMemory<byte>? GetValueByType(BlittableJsonReaderObject.PropertyDetails propertyDetails, object value, PgColumn pgColumn)
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

        public void ReleaseQueryResources()
        {
            _queryOperationContext?.Dispose();
            _queryOperationContext = null;
            _samples = null;
        }
        
        public override void Dispose()
        {
            GC.SuppressFinalize(this);
            ReleaseQueryResources();
        }
    }
}
