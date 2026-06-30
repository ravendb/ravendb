using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Integrations.PostgreSQL.Messages;
using Raven.Server.Integrations.PostgreSQL.Types;
using Raven.Server.Logging;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Integrations.PostgreSQL
{
    public class RqlQuery : PgQuery
    {
        protected readonly DocumentDatabase DocumentDatabase;
        private QueryOperationContext _queryOperationContext;
        private List<Document> _samples;
        private readonly int? _limit;
        private bool _queryWasRun;
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<RqlQuery>();

        private const int SchemaInferenceSampleSize = 1024;

        protected virtual bool IncludeDocumentIdColumn => true;

        protected virtual bool IncludePowerBIJsonColumn => true;

        ~RqlQuery()
        {
            try
            {
                if (Logger.IsWarnEnabled)
                    Logger.Warn($"Query '{QueryString ?? "null"}' wasn't disposed properly.{Environment.NewLine}" +
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

        public override async Task<ICollection<PgColumn>> Init()
        {
            if (IsEmptyQuery)
                return default;

            _samples = await RunRqlQuery();
            return await GenerateSchema();
        }

        public async Task<List<Document>> RunRqlQuery(string forcedQueryToRun = null)
        {
            _queryOperationContext ??= QueryOperationContext.Allocate(DocumentDatabase);
            var parameters = DynamicJsonValue.Convert(Parameters);
            var queryParameters = _queryOperationContext.Documents.ReadObject(parameters, "query/parameters");

            var indexQuery = new IndexQueryServerSide(forcedQueryToRun ?? QueryString, queryParameters);

            // if limit is 0, fetch one document for schema generation; otherwise cap to the query limit
            indexQuery.PageSize = _limit switch
            {
                0 => 1,
                > 0 => Math.Min(_limit.Value, SchemaInferenceSampleSize),
                _ => SchemaInferenceSampleSize
            };

            using var cancelToken = new OperationCancelToken(DocumentDatabase.DatabaseShutdown);

            _queryWasRun = true;
            var documentQueryResult =
                await DocumentDatabase.QueryRunner.ExecuteQuery(indexQuery, _queryOperationContext, null, cancelToken);

            return documentQueryResult.Results;
        }

        // IndexQueryServerSide(string, ...) doesn't carry the parsed LIMIT/OFFSET into PageSize/Start
        // (only the JSON-body Create() path does), so apply the embedded bounds here.
        private void ApplyEmbeddedLimits(IndexQueryServerSide indexQuery)
        {
            if (indexQuery.Metadata.Query.Limit != null)
            {
                var limit = QueryBuilderHelper.GetLongValue(
                    indexQuery.Metadata.Query, indexQuery.Metadata,
                    indexQuery.QueryParameters, indexQuery.Metadata.Query.Limit, int.MaxValue);
                indexQuery.Limit = limit;
                indexQuery.PageSize = Math.Min(limit, indexQuery.PageSize);
            }

            if (indexQuery.Metadata.Query.Offset != null)
            {
                var offset = QueryBuilderHelper.GetLongValue(
                    indexQuery.Metadata.Query, indexQuery.Metadata,
                    indexQuery.QueryParameters, indexQuery.Metadata.Query.Offset, 0);
                indexQuery.Offset = offset;
                indexQuery.Start = Math.Max(offset, indexQuery.Start);
            }
        }

        protected virtual async Task<ICollection<PgColumn>> GenerateSchema()
        {
            if (_samples == null || _samples.Count == 0)
            {
                var query = QueryMetadata.ParseQuery(QueryString, QueryType.Select);

                query.Where = null;
                // Schema discovery samples for type inference - it must not inherit the user's
                // narrow LIMIT/OFFSET. Clearing them lets the sample scan walk up to the in-method
                // cap instead of stopping early and inferring types from too small a window.
                query.Limit = null;
                query.Offset = null;

                var queryWithoutFiltering = query.ToString();

                _samples = await RunRqlQuery(queryWithoutFiltering);

                if (_samples == null || _samples.Count == 0)
                    return Array.Empty<PgColumn>();
            }

            var resultsFormat = GetDefaultResultsFormat();

            if (IncludeDocumentIdColumn && _samples[0].Id != null)
                Columns[PgSyntheticColumns.DocumentId] = new PgColumn(PgSyntheticColumns.DocumentId, (short)Columns.Count, PgText.Default, resultsFormat);

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
            {
                // RQL surfaces the document id as a property named `id()`; the synthetic `id` column was
                // already prepended, so skip the duplicate (a column-count mismatch crashes PowerBI).
                if (PgSyntheticColumns.IsDocumentIdColumn(property)
                    && Columns.ContainsKey(PgSyntheticColumns.DocumentId))
                {
                    uncheckedTypePropertiesNames.Remove(property);
                    continue;
                }

                Columns.TryAdd(property, new PgColumn(property, (short)Columns.Count, PgJson.Default, resultsFormat));
            }

            // Go through results - we'll try to find all properties types.
            for (int sampleIndex = 0; sampleIndex < _samples.Count; sampleIndex++)
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

            if (IncludePowerBIJsonColumn)
            {
                if (Columns.TryGetValue(PgSyntheticColumns.Json, out var jsonColumn))
                {
                    jsonColumn.PgType = PgJson.Default;
                }
                else
                {
                    Columns[PgSyntheticColumns.Json] = new PgColumn(PgSyntheticColumns.Json, (short)Columns.Count, PgJson.Default, resultsFormat);
                }
            }

            return Columns.Values;
        }

        public static bool TryParse(string queryText, int[] parametersDataTypes, DocumentDatabase documentDatabase, out RqlQuery rqlQuery)
        {
            try
            {
                QueryMetadata.ParseQuery(queryText, QueryType.Select);
            }
            catch (Exception e) when (e is InvalidQueryException or QueryParser.ParseException)
            {
                // Input is not valid RQL - leave it for the next dispatch arm (PowerBI / virtual catalog /
                // SQL to RQL translator). Any other exception is a real failure and must propagate.
                if (Logger.IsDebugEnabled)
                    Logger.Debug($"{nameof(RqlQuery)}.{nameof(TryParse)} rejected query as non-RQL: {e.Message}");
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

                if (_limit == 0 || (_samples != null && _samples.Count == 0))
                {
                    await writer.WriteAsync(builder.CommandComplete($"SELECT 0"), token);
                    return;
                }

                // _samples == null means resources were released after a previous Execute (named statement re-execution) - proceed normally

                _queryOperationContext ??= QueryOperationContext.Allocate(DocumentDatabase);

                var parameters = DynamicJsonValue.Convert(Parameters);
                var queryParameters = _queryOperationContext.Documents.ReadObject(parameters, "query/parameters");
                var indexQuery = new IndexQueryServerSide(QueryString, queryParameters);

                if (_limit.HasValue)
                    indexQuery.PageSize = _limit.Value;
                else
                    ApplyEmbeddedLimits(indexQuery);

                await using var streamWriter = new PgStreamDocumentQueryResultWriter(
                    writer, builder, this, Columns, GetDocumentIdColumnIndex(), GetPowerBIJsonColumnIndex(), _queryOperationContext.Documents);

                using var cancelToken = new OperationCancelToken(DocumentDatabase.DatabaseShutdown, token);
                await DocumentDatabase.QueryRunner.ExecuteStreamQuery(indexQuery, _queryOperationContext, NopHttpResponse.Instance, streamWriter, cancelToken);

                await writer.WriteAsync(builder.CommandComplete($"SELECT {streamWriter.Count}"), token);
            }
            finally
            {
                ReleaseQueryResources();
            }
        }

        // Writes one streamed document into the rented row buffer via the per-row hooks. Called by
        // PgStreamDocumentQueryResultWriter for each result; subclasses customize through the hooks
        // (WriteDocumentIdColumn / BeforeRow / HandleSpecialColumnsIfNeeded / AfterRow) rather than
        // by overriding the streaming mechanics.
        internal void WriteRow(Document result, ReadOnlyMemory<byte>?[] row, short? idIndex, short? jsonIndex, DocumentsOperationContext context)
        {
            var jsonResult = result.Data;

            WriteDocumentIdColumn(result, row, idIndex);

            var modifications = BeforeRow(jsonResult, jsonIndex);

            BlittableJsonReaderObject.PropertyDetails prop = default;
            foreach (var (columnName, pgColumn) in Columns)
            {
                var index = jsonResult.GetPropertyIndex(columnName);
                if (index == -1)
                    continue;

                jsonResult.GetPropertyByIndex(index, ref prop);

                row[pgColumn.ColumnIndex] = GetValueByType(prop, prop.Value, pgColumn);

                HandleSpecialColumnsIfNeeded(columnName, prop, prop.Value, row);

                modifications?.Remove(columnName);
            }

            AfterRow(jsonResult, row, jsonIndex, context);
        }

        protected short? GetDocumentIdColumnIndex()
        {
            if (IncludeDocumentIdColumn == false)
                return null;

            return Columns.TryGetValue(PgSyntheticColumns.DocumentId, out var col)
                ? col.ColumnIndex
                : null;
        }

        protected short? GetPowerBIJsonColumnIndex()
        {
            if (IncludePowerBIJsonColumn == false)
                return null;

            return Columns.TryGetValue(PgSyntheticColumns.Json, out var jsonCol)
                ? jsonCol.ColumnIndex
                : null;
        }

        protected void WriteDocumentIdColumn(Document result, ReadOnlyMemory<byte>?[] row, short? idIndex)
        {
            if (idIndex != null && result.Id != null)
                row[idIndex.Value] = Encoding.UTF8.GetBytes(result.Id.ToString());
        }

        protected virtual DynamicJsonValue BeforeRow(BlittableJsonReaderObject jsonResult, short? jsonIndex)
        {
            if (IncludePowerBIJsonColumn == false || jsonIndex == null)
                return null;

            jsonResult.Modifications = new DynamicJsonValue(jsonResult);

            if (jsonResult.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject _))
            {
                // remove @metadata
                jsonResult.Modifications.Remove(Constants.Documents.Metadata.Key);
            }

            return jsonResult.Modifications;
        }

        protected virtual void HandleSpecialColumnsIfNeeded(string columnName, BlittableJsonReaderObject.PropertyDetails property, object value, ReadOnlyMemory<byte>?[] row)
        {
        }

        protected virtual void AfterRow(BlittableJsonReaderObject jsonResult, ReadOnlyMemory<byte>?[] row, short? jsonIndex, DocumentsOperationContext context)
        {
            if (IncludePowerBIJsonColumn == false || jsonIndex == null)
                return;

            if (jsonResult.Modifications == null)
                return;

            if (jsonResult.Modifications.Removals.Count == jsonResult.Count)
                return;

            using var modified = context.ReadObject(jsonResult, "renew");
            row[jsonIndex.Value] = Encoding.UTF8.GetBytes(modified.ToString());
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

                // Column PgType is fixed at schema-inference time, but RavenDB docs in one
                // collection can mix numeric types (Integer + LazyNumber). Coerce instead of
                // dropping the row.
                case (BlittableJsonToken.Integer, PgTypeOIDs.Float8):
                    // long -> double is lossless up to 2^53.
                    return pgColumn.PgType.ToBytes((double)(long)value, pgColumn.FormatCode);

                case (BlittableJsonToken.LazyNumber, PgTypeOIDs.Int8):
                    // Lossy fractional narrowing - better than rendering blank.
                    return pgColumn.PgType.ToBytes((long)(double)(LazyNumberValue)value, pgColumn.FormatCode);

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
                    // Must pass CultureInfo.InvariantCulture explicitly - `.` is the JSON-native
                    // decimal separator, but `double.Parse(string)` honors the current culture.
                    // Without this, a server running under a locale with a comma decimal separator
                    // (de-DE, pl-PL, fr-FR, etc.) throws FormatException mid-row write and corrupts
                    // the wire protocol. The explicit `(string)` cast disambiguates LazyStringValue's
                    // overloaded implicit conversion (it also has ReadOnlySpan<byte>).
                    return pgColumn.PgType.ToBytes(
                        double.Parse((string)(LazyStringValue)value, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture),
                        pgColumn.FormatCode);

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
