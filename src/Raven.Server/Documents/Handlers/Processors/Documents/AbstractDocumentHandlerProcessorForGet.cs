using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.WebUtilities;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Revisions;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class AbstractDocumentHandlerProcessorForGet<TRequestHandler, TOperationContext, TDocumentType> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    // ReSharper disable once StaticMemberInGenericType
    private static readonly (long, long) NoResults = (-1, -1);

    private readonly HttpMethod _method;

    protected readonly List<IDisposable> Disposables = new();

    protected AbstractDocumentHandlerProcessorForGet(HttpMethod method, [NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
        if (method != HttpMethod.Get && method != HttpMethod.Post)
            throw new InvalidOperationException($"The processor is supposed to handle GET and POST methods while '{method}' was specified");

        _method = method;
    }

    protected abstract bool SupportsShowingRequestInTrafficWatch { get; }

    protected abstract CancellationToken CancellationToken { get; }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            await ExecuteInternalAsync(context);
        }
    }

    protected virtual async ValueTask ExecuteInternalAsync(TOperationContext context)
    {
        var sw = Stopwatch.StartNew();

        var parameters = QueryStringParameters.Create(RequestHandler.HttpContext.Request);

        if (_method == HttpMethod.Get)
        {
            // no-op - this was parses via QueryStringParameters few lines up
        }
        else if (_method == HttpMethod.Post)
            parameters.Ids = await GetIdsFromRequestBodyAsync(context, RequestHandler);
        else
            throw new NotSupportedException($"Unhandled method type: {_method}");

        if (SupportsShowingRequestInTrafficWatch && TrafficWatchManager.HasRegisteredClients)
            RequestHandler.AddStringToHttpContext(IdsToString(parameters.Ids), TrafficWatchChangeType.Documents);

        (long NumberOfResults, long TotalDocumentsSizeInBytes) responseWriteStats;
        int pageSize;
        string actionName;

        if (parameters.Ids is { Count: > 0 })
        {
            pageSize = parameters.Ids.Count;
            actionName = nameof(GetDocumentsByIdAsync);

            var etag = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);

            // includes
            var revisions = GetRevisionsToInclude(parameters);
            var timeSeries = GetTimeSeriesToInclude(parameters);

            responseWriteStats = await GetDocumentsByIdAsync(context, parameters, revisions, timeSeries, etag);
        }
        else
        {
            pageSize = RequestHandler.GetPageSize();
            actionName = nameof(GetDocumentsAsync);

            var changeVector = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);
            var etag = RequestHandler.GetLongQueryString("etag", false);

            var isStartsWith = HttpContext.Request.Query.ContainsKey("startsWith");

            StartsWithParams startsWithParams = null;

            if (isStartsWith)
            {
                startsWithParams = new StartsWithParams
                {
                    IdPrefix = HttpContext.Request.Query["startsWith"],
                    Matches = HttpContext.Request.Query["matches"],
                    Exclude = HttpContext.Request.Query["exclude"],
                    StartAfterId = HttpContext.Request.Query["startAfter"],
                };
            }

            responseWriteStats = await GetDocumentsAsync(context, etag, startsWithParams, parameters.MetadataOnly, changeVector);
        }

        if (responseWriteStats != NoResults)
        {
            if (RequestHandler.ShouldAddPagingPerformanceHint(responseWriteStats.NumberOfResults))
            {
                string details;

                if (parameters.Ids is { Count: > 0 })
                    details = CreatePerformanceHintDetails();
                else
                    details = HttpContext.Request.QueryString.Value;

                RequestHandler.AddPagingPerformanceHint(
                    PagingOperationType.Documents,
                    actionName,
                    details,
                    responseWriteStats.NumberOfResults,
                    pageSize,
                    sw.ElapsedMilliseconds,
                    responseWriteStats.TotalDocumentsSizeInBytes);
            }
        }

        string CreatePerformanceHintDetails()
        {
            var sb = new StringBuilder();
            var addedIdsCount = 0;
            var first = true;

            while (sb.Length < 1024 && addedIdsCount < parameters.Ids.Count)
            {
                if (first == false)
                    sb.Append(", ");
                else
                    first = false;

                sb.Append($"{parameters.Ids[addedIdsCount++]}");
            }

            var idsLeftCount = parameters.Ids.Count - addedIdsCount;

            if (idsLeftCount > 0)
            {
                sb.Append($" ... (and {idsLeftCount} more)");
            }

            return sb.ToString();
        }

        static string IdsToString(List<ReadOnlyMemory<char>> ids)
        {
            if (ids == null || ids.Count == 0)
                return string.Empty;

            var sb = new StringBuilder();
            for (int i = 0; i < ids.Count; i++)
            {
                if (i != 0)
                    sb.Append(",");

                ReadOnlyMemory<char> id = ids[i];
                sb.Append(id.ToString());
            }

            return sb.ToString();
        }
    }

    protected async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> GetDocumentsByIdAsync(TOperationContext context,
        QueryStringParameters parameters, RevisionIncludeField revisions, HashSet<AbstractTimeSeriesRange> timeSeries, string etag)
    {
        var clusterWideTx = parameters.TxMode == TransactionMode.ClusterWide;
        var result = await GetDocumentsByIdImplAsync(context, parameters.Ids, parameters.IncludePaths, revisions, parameters.Counters, timeSeries, parameters.CompareExchange, parameters.MetadataOnly, clusterWideTx, etag)
                                .ConfigureAwait(false);

        if (result.StatusCode == HttpStatusCode.NotFound)
        {
            if (etag == HttpCache.NotFoundResponse)
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            else
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;

            return NoResults;
        }

        if (etag == result.Etag)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;

            return NoResults;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + result.Etag + "\"";

        return await WriteDocumentsByIdResultAsync(context, parameters.MetadataOnly, clusterWideTx, result)
                        .ConfigureAwait(false);
    }

    private async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsByIdResultAsync(
        TOperationContext context, bool metadataOnly, bool clusterWideTx, DocumentsByIdResult<TDocumentType> result)
    {
        long numberOfResults;
        long totalDocumentsSizeInBytes;
        await using (AsyncBlittableJsonTextWriter.Create(context, RequestHandler.ResponseBodyStream(), out var writer))
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(GetDocumentsResult.Results));

            (numberOfResults, totalDocumentsSizeInBytes) = await WriteDocumentsAsync(writer, context, result.Documents, metadataOnly, CancellationToken);

            writer.WriteComma();

            await WriteIncludesAsync(writer, context, nameof(GetDocumentsResult.Includes), result.Includes, CancellationToken)
                    .ConfigureAwait(false);

            if (result.CounterIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.CounterIncludes));
                await result.CounterIncludes.WriteIncludesAsync(writer, context, CancellationToken)
                                            .ConfigureAwait(false);
            }

            if (result.TimeSeriesIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.TimeSeriesIncludes));
                await result.TimeSeriesIncludes.WriteIncludesAsync(writer, context, CancellationToken)
                                               .ConfigureAwait(false);
            }

            if (result.RevisionIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.RevisionIncludes));
                writer.WriteStartArray();
                await result.RevisionIncludes.WriteIncludesAsync(writer, context, CancellationToken)
                                             .ConfigureAwait(false);
                writer.WriteEndArray();
            }

            if (result.CompareExchangeIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.CompareExchangeValueIncludes));
                await writer.WriteCompareExchangeValuesAsync(result.CompareExchangeIncludes, CancellationToken)
                                .ConfigureAwait(false);
            }

            writer.WriteEndObject();
        }
        return (numberOfResults, totalDocumentsSizeInBytes);
    }

    protected abstract ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsAsync(AsyncBlittableJsonTextWriter writer,
        TOperationContext context,
        IEnumerable<TDocumentType> documentsToWrite, bool metadataOnly, CancellationToken token);

    protected abstract ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsAsync(AsyncBlittableJsonTextWriter writer,
        TOperationContext context,
        IAsyncEnumerable<TDocumentType> documentsToWrite, bool metadataOnly, CancellationToken token);

    protected abstract ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, TOperationContext context, string propertyName,
        List<TDocumentType> includes, CancellationToken token);

    protected abstract ValueTask<DocumentsByIdResult<TDocumentType>> GetDocumentsByIdImplAsync(
        TOperationContext context,
        List<ReadOnlyMemory<char>> ids,
        StringValues includePaths,
        RevisionIncludeField revisions,
        StringValues counters,
        HashSet<AbstractTimeSeriesRange> timeSeries,
        StringValues compareExchangeValues,
        bool metadataOnly,
        bool clusterWideTx,
        string etag);

    protected async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> GetDocumentsAsync(TOperationContext context, long? etag, StartsWithParams startsWith, bool metadataOnly, string changeVector)
    {
        var result = await GetDocumentsImplAsync(context, etag, startsWith, changeVector);

        if (changeVector == result.Etag)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;

            return NoResults;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + result.Etag + "\"";

        long numberOfResults;
        long totalDocumentsSizeInBytes;

        await using (AsyncBlittableJsonTextWriter.Create(context, RequestHandler.ResponseBodyStream(), out var writer))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("Results");

            if (result.DocumentsAsync != null)
            {
                (numberOfResults, totalDocumentsSizeInBytes) = await WriteDocumentsAsync(writer, context, result.DocumentsAsync, metadataOnly, CancellationToken)
                                                                         .ConfigureAwait(false);
            }
            else
            {
                (numberOfResults, totalDocumentsSizeInBytes) = await WriteDocumentsAsync(writer, context, result.Documents, metadataOnly, CancellationToken)
                                                                        .ConfigureAwait(false);
            }

            if (result.ContinuationToken != null)
            {
                writer.WriteComma();
                writer.WriteContinuationToken(context, result.ContinuationToken);
            }

            writer.WriteEndObject();
        }

        return (numberOfResults, totalDocumentsSizeInBytes);
    }

    protected abstract ValueTask<DocumentsResult> GetDocumentsImplAsync(TOperationContext context, long? etag, StartsWithParams startsWith, string changeVector);

    private static RevisionIncludeField GetRevisionsToInclude(QueryStringParameters parameters)
    {
        if (parameters.Revisions == null && parameters.RevisionsBefore == null)
            return null;

        var rif = new RevisionIncludeField();

        if (parameters.RevisionsBefore.HasValue && DateTime.TryParseExact(parameters.RevisionsBefore.Value.Span, DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dateTime))
            rif.RevisionsBeforeDateTime = dateTime.ToUniversalTime();

        if (parameters.Revisions != null)
        {
            foreach (var changeVector in parameters.Revisions)
                rif.RevisionsChangeVectorsPaths.Add(changeVector.ToString());
        }

        return rif;
    }

    private HashSet<AbstractTimeSeriesRange> GetTimeSeriesToInclude(QueryStringParameters parameters)
    {
        if (parameters.TimeSeries == null && parameters.TimeSeriesTimes == null && parameters.TimeSeriesCounts == null)
            return null;

        if (parameters.TimeSeries is { Count: > 1 } && parameters.TimeSeriesHasAllTimeSeries)
            throw new InvalidOperationException($"Cannot have more than one include on '{Constants.TimeSeries.All}'.");
        if (parameters.TimeSeriesTimes is { Count: > 1 } && parameters.TimeSeriesTimesHasAllTimeSeries)
            throw new InvalidOperationException($"Cannot have more than one include on '{Constants.TimeSeries.All}'.");
        if (parameters.TimeSeriesCounts is { Count: > 1 } && parameters.TimeSeriesCountsHasAllTimeSeries)
            throw new InvalidOperationException($"Cannot have more than one include on '{Constants.TimeSeries.All}'.");

        var timeSeriesCount = parameters.TimeSeries?.Count ?? 0;
        if (timeSeriesCount != parameters.From.Count || parameters.From.Count != parameters.To.Count)
            throw new InvalidOperationException("Parameters 'timeseriesNames', 'fromList' and 'toList' must be of equal length. " +
                                                $"Got : timeseriesNames.Count = {timeSeriesCount}, fromList.Count = {parameters.From.Count}, toList.Count = {parameters.To.Count}.");

        var timeSeriesTimesCount = parameters.TimeSeriesTimes?.Count ?? 0;
        if (timeSeriesTimesCount != parameters.TimeTypes.Count || parameters.TimeTypes.Count != parameters.TimeValues.Count || parameters.TimeValues.Count != parameters.TimeUnits.Count)
            throw new InvalidOperationException($"Parameters 'timeseriesTime', 'timeType', 'timeValue' and 'timeUnit' must be of equal length. " +
                                                $"Got : timeseriesTime.Count = {timeSeriesTimesCount}, timeType.Count = {parameters.TimeTypes.Count}, timeValue.Count = {parameters.TimeValues.Count}, timeUnit.Count = {parameters.TimeUnits.Count}.");

        var timeSeriesCountsCount = parameters.TimeSeriesCounts?.Count ?? 0;
        if (timeSeriesCountsCount != parameters.CountTypes.Count || parameters.CountTypes.Count != parameters.CountValues.Count)
            throw new InvalidOperationException($"Parameters 'timeseriesCount', 'countType', 'countValue' must be of equal length. " +
                                                $"Got : timeseriesCount.Count = {timeSeriesCountsCount}, countType.Count = {parameters.CountTypes}, countValue.Count = {parameters.CountValues.Count}.");

        var hs = new HashSet<AbstractTimeSeriesRange>(AbstractTimeSeriesRangeComparer.Instance);

        if (parameters.TimeSeries is { Count: > 0 })
        {
            for (int i = 0; i < parameters.TimeSeries.Count; i++)
            {
                hs.Add(new TimeSeriesRange
                {
                    Name = parameters.TimeSeries[i].ToString(),
                    From = string.IsNullOrEmpty(parameters.From[i])
                        ? DateTime.MinValue
                        : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(parameters.From[i], "from"),
                    To = string.IsNullOrEmpty(parameters.To[i])
                        ? DateTime.MaxValue
                        : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(parameters.To[i], "to")
                });
            }
        }

        if (parameters.TimeSeriesTimes is { Count: > 0 })
        {
            for (int i = 0; i < parameters.TimeSeriesTimes.Count; i++)
            {
                var timeValueUnit = (TimeValueUnit)Enum.Parse(typeof(TimeValueUnit), parameters.TimeUnits[i]);
                if (timeValueUnit == TimeValueUnit.None)
                    throw new InvalidOperationException(
                        $"Got unexpected {nameof(TimeValueUnit)} '{nameof(TimeValueUnit.None)}'. Only the following are supported: '{nameof(TimeValueUnit.Second)}' or '{nameof(TimeValueUnit.Month)}'.");

                if (int.TryParse(parameters.TimeValues[i], out int res) == false)
                    throw new InvalidOperationException($"Could not parse timeseries time range value.");

                hs.Add(new TimeSeriesTimeRange
                {
                    Name = parameters.TimeSeriesTimes[i].ToString(),
                    Type = (TimeSeriesRangeType)Enum.Parse(typeof(TimeSeriesRangeType), parameters.TimeTypes[i]),
                    Time = timeValueUnit == TimeValueUnit.Second ? TimeValue.FromSeconds(res) : TimeValue.FromMonths(res)
                });
            }
        }

        if (parameters.TimeSeriesCounts is { Count: > 0 })
        {
            for (int i = 0; i < parameters.TimeSeriesCounts.Count; i++)
            {
                if (int.TryParse(parameters.CountValues[i], out int res) == false)
                    throw new InvalidOperationException($"Could not parse timeseries count value.");

                hs.Add(new TimeSeriesCountRange
                {
                    Name = parameters.TimeSeriesCounts[i].ToString(),
                    Type = (TimeSeriesRangeType)Enum.Parse(typeof(TimeSeriesRangeType), parameters.CountTypes[i]),
                    Count = res
                });
            }
        }

        return hs;
    }

    public override void Dispose()
    {
        base.Dispose();

        for (int i = Disposables.Count - 1; i >= 0; i--)
        {
            Disposables[i].Dispose();
        }
    }

    private static async ValueTask<List<ReadOnlyMemory<char>>> GetIdsFromRequestBodyAsync(TOperationContext context, TRequestHandler requestHandler)
    {
        var docs = await context.ReadForMemoryAsync(requestHandler.RequestBodyStream(), "docs");
        if (docs.TryGet("Ids", out BlittableJsonReaderArray array) == false)
            Web.RequestHandler.ThrowRequiredPropertyNameInRequest("Ids");

        var idsAsStrings = new List<ReadOnlyMemory<char>>(array.Length);

        for (int i = 0; i < array.Length; i++)
        {
            var id = array.GetStringByIndex(i);
            idsAsStrings.Add(id.AsMemory());
        }

        return idsAsStrings;
    }

    protected sealed class DocumentsByIdResult<T>
    {
        public List<T> Documents { get; set; }

        public List<T> Includes { get; set; }

        public IRevisionIncludes RevisionIncludes { get; set; }

        public ICounterIncludes CounterIncludes { get; set; }

        public ITimeSeriesIncludes TimeSeriesIncludes { get; set; }

        public Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> CompareExchangeIncludes { get; set; }

        public HashSet<string> MissingIncludes { get; set; }

        public string Etag { get; set; }

        public HttpStatusCode StatusCode { get; set; } = HttpStatusCode.OK;
    }

    protected sealed class DocumentsResult
    {
        public IAsyncEnumerable<TDocumentType> DocumentsAsync { get; set; }

        public IEnumerable<TDocumentType> Documents { get; set; }

        public ShardedPagingContinuation ContinuationToken { get; set; }

        public string Etag { get; set; }
    }

    protected sealed class StartsWithParams
    {
        public string IdPrefix { get; set; }

        public string Matches { get; set; }

        public string Exclude { get; set; }

        public string StartAfterId { get; set; }
    }

    protected sealed class QueryStringParameters : AbstractQueryStringParameters
    {
        public bool MetadataOnly;

        public StringValues IncludePaths;

        public List<ReadOnlyMemory<char>> Ids;

        public StringValues Counters;

        public List<ReadOnlyMemory<char>> Revisions;

        public ReadOnlyMemory<char>? RevisionsBefore;

        public List<ReadOnlyMemory<char>> TimeSeries;

        public bool TimeSeriesHasAllTimeSeries;

        public List<ReadOnlyMemory<char>> TimeSeriesTimes;

        public bool TimeSeriesTimesHasAllTimeSeries;

        public List<ReadOnlyMemory<char>> TimeSeriesCounts;

        public bool TimeSeriesCountsHasAllTimeSeries;

        public StringValues From;

        public StringValues To;

        public StringValues TimeTypes;

        public StringValues TimeValues;

        public StringValues TimeUnits;

        public StringValues CountTypes;

        public StringValues CountValues;

        public StringValues CompareExchange;

        public TransactionMode TxMode;

        private readonly bool _isGet;

        private QueryStringParameters([NotNull] HttpRequest httpRequest)
            : base(httpRequest)
        {
            _isGet = httpRequest.Method == HttpMethods.Get;
        }

        protected override void OnFinalize()
        {
            if (AnyStringValues() == false)
                return;

            IncludePaths = ConvertToStringValues("include");
            Counters = ConvertToStringValues("counter");
            CompareExchange = ConvertToStringValues("cmpxchg");
            From = ConvertToStringValues("from");
            To = ConvertToStringValues("to");
            TimeTypes = ConvertToStringValues("timeType");
            TimeValues = ConvertToStringValues("timeValue");
            TimeUnits = ConvertToStringValues("timeUnit");
            CountTypes = ConvertToStringValues("countType");
            CountValues = ConvertToStringValues("countValue");
        }

        protected override void OnValue(QueryStringEnumerable.EncodedNameValuePair pair)
        {
            var name = pair.EncodedName;

            if (_isGet && IsMatch(name, IdQueryStringName))
            {
                Ids ??= new List<ReadOnlyMemory<char>>(1);
                Ids.Add(pair.DecodeValue());
                return;
            }

            if (IsMatch(name, MetadataOnlyQueryStringName))
            {
                MetadataOnly = GetBoolValue(name, pair.EncodedValue);
                return;
            }

            if (IsMatch(name, IncludesQueryStringName))
            {
                // optimize this
                AddForStringValues("include", pair.DecodeValue());
                return;
            }

            if (IsMatch(name, CmpxchgQueryStringName))
            {
                // optimize this
                AddForStringValues("cmpxchg", pair.DecodeValue());
                return;
            }

            if (IsMatch(name, CounterQueryStringName))
            {
                // optimize this
                AddForStringValues("counter", pair.DecodeValue());
                return;
            }

            if (IsMatch(name, RevisionsQueryStringName))
            {
                Revisions ??= new List<ReadOnlyMemory<char>>(1);
                Revisions.Add(pair.DecodeValue());
                return;
            }

            if (IsMatch(name, RevisionsBeforeQueryStringName))
            {
                RevisionsBefore = pair.DecodeValue();
                return;
            }

            if (IsMatch(name, TimeSeriesQueryStringName))
            {
                TimeSeries ??= new List<ReadOnlyMemory<char>>(1);

                var value = pair.DecodeValue();
                if (value.Span.Equals(AllTimeSeries.Span, StringComparison.Ordinal))
                    TimeSeriesHasAllTimeSeries = true;

                TimeSeries.Add(value);
                return;
            }

            if (IsMatch(name, TimeSeriesTimesQueryStringName))
            {
                TimeSeriesTimes ??= new List<ReadOnlyMemory<char>>(1);

                var value = pair.DecodeValue();
                if (value.Span.Equals(AllTimeSeries.Span, StringComparison.Ordinal))
                    TimeSeriesTimesHasAllTimeSeries = true;

                TimeSeriesTimes.Add(value);
                return;
            }

            if (IsMatch(name, TimeSeriesCountsQueryStringName))
            {
                TimeSeriesCounts ??= new List<ReadOnlyMemory<char>>(1);

                var value = pair.DecodeValue();
                if (value.Span.Equals(AllTimeSeries.Span, StringComparison.Ordinal))
                    TimeSeriesCountsHasAllTimeSeries = true;

                TimeSeriesCounts.Add(value);
                return;
            }

            if (IsMatch(name, TxModeQueryStringName))
            {
                if (TryGetEnumValue<TransactionMode>(pair.EncodedValue, out var value))
                    TxMode = value;

                return;
            }

            if (IsMatch(name, FromQueryStringName))
            {
                // optimize this
                AddForStringValues("from", pair.DecodeValue());
                return;
            }

            if (IsMatch(name, ToQueryStringName))
            {
                // optimize this
                AddForStringValues("to", pair.DecodeValue());
                return;
            }

            if (IsMatch(name, TimeTypeQueryStringName))
            {
                // optimize this
                AddForStringValues("timeType", pair.DecodeValue());
                return;
            }

            if (IsMatch(name, TimeValueQueryStringName))
            {
                // optimize this
                AddForStringValues("timeValue", pair.DecodeValue());
                return;
            }

            if (IsMatch(name, TimeUnitQueryStringName))
            {
                // optimize this
                AddForStringValues("timeUnit", pair.DecodeValue());
                return;
            }

            if (IsMatch(name, CountTypeQueryStringName))
            {
                // optimize this
                AddForStringValues("countType", pair.DecodeValue());
                return;
            }

            if (IsMatch(name, CountValueQueryStringName))
            {
                // optimize this
                AddForStringValues("countValue", pair.DecodeValue());
                return;
            }
        }

        public static QueryStringParameters Create(HttpRequest httpRequest)
        {
            var parameters = new QueryStringParameters(httpRequest);
            parameters.Parse();

            return parameters;
        }
    }

}
