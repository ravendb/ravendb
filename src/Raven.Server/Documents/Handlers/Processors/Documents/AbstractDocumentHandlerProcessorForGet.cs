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
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class
    AbstractDocumentHandlerProcessorForGet<TRequestHandler, TOperationContext, TDocumentType> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TOperationContext : JsonOperationContext
{
    // ReSharper disable once StaticMemberInGenericType
    private static readonly (long, long) NoResults = (-1, -1);

    private readonly HttpMethod _method;

    [CanBeNull]
    private readonly List<ReadOnlyMemory<char>> _ids;

    protected AbstractDocumentHandlerProcessorForGet(HttpMethod method, [NotNull] TRequestHandler requestHandler, [CanBeNull] List<ReadOnlyMemory<char>> ids = null) : base(requestHandler)
    {
        if (method != HttpMethod.Get && method != HttpMethod.Post)
            throw new InvalidOperationException($"The processor is supposed to handle GET and POST methods while '{method}' was specified");

        _method = method;
        _ids = ids;
    }

    protected abstract bool SupportsShowingRequestInTrafficWatch { get; }

    protected abstract CancellationToken CancellationToken { get; }

    public sealed override ValueTask ExecuteAsync()
    {
        // The reason behind this is to avoid awaiting execution in the handler and do it directly in the router code.
        // This reduces AsyncStateMachine size by avoiding creating state in the handler code.
        RegisterForDisposal(this);
            
        // For the context, allocate only if it was not previously allocated by the caller.
        // If not provided by the caller, we create one and wrap it in borrowable to pass down to async path, if needed.
        // If provided, we don't scope it cause the caller owns it.
        TOperationContext context = GetContextScopedToRequest();
        
        var sw = Stopwatch.StartNew();

        var parameters = QueryStringParameters.Create(RequestHandler.HttpContext.Request);

        if (_method == HttpMethod.Get)
        {
            // no-op - this was parses via QueryStringParameters few lines up
        }
        else if (_method == HttpMethod.Post)
            parameters.Ids = _ids;
        else
            return ValueTask.FromException(new NotSupportedException($"Unhandled method type: {_method}"));

        if (SupportsShowingRequestInTrafficWatch && TrafficWatchManager.HasRegisteredClients)
            RequestHandler.AddStringToHttpContext(IdsToString(parameters.Ids), TrafficWatchChangeType.Documents);

        int pageSize;
        string actionName;

        ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> getDocumentsAsync;
        if (parameters.Ids is { Count: > 0 })
        {
            pageSize = parameters.Ids.Count;
            actionName = nameof(GetDocumentsByIdAsync);

            var etag = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);

            // includes
            var revisions = GetRevisionsToInclude(parameters);
            var timeSeries = GetTimeSeriesToInclude(parameters);

            getDocumentsAsync = GetDocumentsByIdAsync(context, parameters, revisions, timeSeries, etag);
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

            getDocumentsAsync = GetDocumentsAsync(context, etag, startsWithParams, parameters.MetadataOnly, changeVector);
        }

        if (getDocumentsAsync.IsCompletedSuccessfully)
        {
            HandleGetDocumentResult(getDocumentsAsync.Result, parameters, actionName, pageSize, sw);
            return ValueTask.CompletedTask;
        }
            
        // Slow async path requires careful scope considerations that are delegated with RegisterForDisposal and GetOrLeaseScopedOperationContext
        return HandleGetDocumentResultAsync(getDocumentsAsync, parameters, actionName, pageSize, sw);
            
        static string IdsToString(List<ReadOnlyMemory<char>> ids)
        {
            if (ids == null || ids.Count == 0)
                return string.Empty;

            var sb = new StringBuilder();
            for (int i = 0; i < ids.Count; i++)
            {
                if (i != 0)
                    sb.Append(',');

                ReadOnlyMemory<char> id = ids[i];
                sb.Append(id);
            }

            return sb.ToString();
        }
    }

    private async ValueTask HandleGetDocumentResultAsync(ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> responseWriteStats,
        QueryStringParameters parameters, string actionName, int pageSize,
        Stopwatch sw)
    {
        HandleGetDocumentResult(await responseWriteStats, parameters, actionName, pageSize, sw);
    }
    
    private void HandleGetDocumentResult((long NumberOfResults, long TotalDocumentsSizeInBytes) responseWriteStats, QueryStringParameters parameters, string actionName, int pageSize,
        Stopwatch sw)
    {
        if (responseWriteStats != NoResults)
        {
            if (RequestHandler.ShouldAddPagingPerformanceHint(responseWriteStats.NumberOfResults))
            {
                string details = parameters.Ids is { Count: > 0 } ? CreatePerformanceHintDetails(parameters) : HttpContext.Request.QueryString.Value;

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

        static string CreatePerformanceHintDetails(QueryStringParameters parameters)
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
    }

    protected async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> GetDocumentsByIdAsync(TOperationContext context,
        QueryStringParameters parameters, RevisionIncludeField revisions, HashSet<AbstractTimeSeriesRange> timeSeries, string etag)
    {
        var clusterWideTx = parameters.TxMode == TransactionMode.ClusterWide;
        var getDocumentsByIdImplAsyncTask = GetDocumentsByIdImplAsync(context, parameters.Ids, parameters.IncludePaths, revisions, parameters.Counters, timeSeries,
            parameters.CompareExchange, parameters.MetadataOnly, clusterWideTx, etag);

        var result = getDocumentsByIdImplAsyncTask.IsCompletedSuccessfully 
            ? getDocumentsByIdImplAsyncTask.Result 
            : await getDocumentsByIdImplAsyncTask;

        using var _ = result.ReadTransaction;

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

        var writeDocumentsByIdResult = WriteDocumentsByIdResultAsync(context, parameters.MetadataOnly, clusterWideTx, result);

        return writeDocumentsByIdResult.IsCompletedSuccessfully
            ? writeDocumentsByIdResult.Result
            : await writeDocumentsByIdResult;
    }

    private async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsByIdResultAsync(
        TOperationContext context, bool metadataOnly, bool clusterWideTx, DocumentsByIdResult<TDocumentType> result)
    {
        long numberOfResults;
        long totalDocumentsSizeInBytes;
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), CancellationToken))
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(GetDocumentsResult.Results));

            var writeDocumentsValueTaskAsync = WriteDocumentsAsync(writer, context, result.Documents, metadataOnly, CancellationToken);
            
            (numberOfResults, totalDocumentsSizeInBytes) = writeDocumentsValueTaskAsync.IsCompletedSuccessfully 
                ? writeDocumentsValueTaskAsync.Result 
                : await writeDocumentsValueTaskAsync;


            writer.WriteComma();

            var includeAsync = WriteIncludesAsync(writer, context, nameof(GetDocumentsResult.Includes), result.Includes, CancellationToken);
            if (includeAsync.IsCompletedSuccessfully == false)
                await includeAsync;

            if (result.CounterIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.CounterIncludes));
                var writeCounterIncludes = result.CounterIncludes.WriteIncludesAsync(writer, context, CancellationToken);
                if (writeCounterIncludes.IsCompletedSuccessfully == false)    
                    await writeCounterIncludes;
            }

            if (result.TimeSeriesIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.TimeSeriesIncludes));
                var writeTimeSeries = result.TimeSeriesIncludes.WriteIncludesAsync(writer, context, CancellationToken);
                if (writeTimeSeries.IsCompletedSuccessfully == false)
                    await writeTimeSeries;
            }

            if (result.RevisionIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.RevisionIncludes));
                writer.WriteStartArray();
                var writeRevisionIncludes = result.RevisionIncludes.WriteIncludesAsync(writer, context, CancellationToken);
                if (writeRevisionIncludes.IsCompletedSuccessfully == false)
                    await writeRevisionIncludes;
                writer.WriteEndArray();
            }

            if (result.CompareExchangeIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.CompareExchangeValueIncludes));
                var writeCompareExchange = writer.WriteCompareExchangeValuesAsync(result.CompareExchangeIncludes, CancellationToken);
                if (writeCompareExchange.IsCompletedSuccessfully == false)
                    await writeCompareExchange;
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

    protected abstract ValueTask<(long Count, long SizeInBytes)> WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, TOperationContext context, string propertyName,
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

    protected async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> GetDocumentsAsync(TOperationContext context, long? etag,
        StartsWithParams startsWith, bool metadataOnly, string changeVector)
    {
        var getDocuments = GetDocumentsImplAsync(context, etag, startsWith, changeVector);
        var result = getDocuments.IsCompletedSuccessfully ? getDocuments.Result : await getDocuments;

        using var _ = result.ReadTransaction;

        if (changeVector == result.Etag)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;

            return NoResults;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + result.Etag + "\"";

        long numberOfResults;
        long totalDocumentsSizeInBytes;

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), CancellationToken))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("Results");

            if (result.DocumentsAsync != null)
            {
                (numberOfResults, totalDocumentsSizeInBytes) = await WriteDocumentsAsync(writer, context, result.DocumentsAsync, metadataOnly, CancellationToken);
            }
            else
            {
                var writeDocuments = WriteDocumentsAsync(writer, context, result.Documents, metadataOnly, CancellationToken);

                (numberOfResults, totalDocumentsSizeInBytes) = writeDocuments.IsCompletedSuccessfully
                    ? writeDocuments.Result
                    : await writeDocuments;

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

        if (parameters.RevisionsBefore.HasValue && DateTime.TryParseExact(parameters.RevisionsBefore.Value.Span, DefaultFormat.DateTimeFormatsToRead,
                CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dateTime))
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
        if (timeSeriesCount != (parameters.From?.Count ?? 0) || (parameters.From?.Count ?? 0) != (parameters.To?.Count ?? 0))
            throw new InvalidOperationException("Parameters 'timeseriesNames', 'fromList' and 'toList' must be of equal length. " +
                                                $"Got : timeseriesNames.Count = {timeSeriesCount}, fromList.Count = {parameters.From?.Count ?? 0}, toList.Count = {parameters.To?.Count ?? 0}.");
        
        var timeSeriesTimesCount = parameters.TimeSeriesTimes?.Count ?? 0;
        if (timeSeriesTimesCount != (parameters.TimeTypes?.Count ?? 0)
            || (parameters.TimeTypes?.Count ?? 0) != (parameters.TimeValues?.Count ?? 0)
            || (parameters.TimeValues?.Count ?? 0) != (parameters.TimeUnits?.Count ?? 0))
            throw new InvalidOperationException($"Parameters 'timeseriesTime', 'timeType', 'timeValue' and 'timeUnit' must be of equal length. " +
                                                $"Got : timeseriesTime.Count = {timeSeriesTimesCount}, timeType.Count = {parameters.TimeTypes?.Count ?? 0}, timeValue.Count = {parameters.TimeValues?.Count ?? 0}, timeUnit.Count = {parameters.TimeUnits?.Count ?? 0}.");

        var timeSeriesCountsCount = parameters.TimeSeriesCounts?.Count ?? 0;
        if (timeSeriesCountsCount != (parameters.CountTypes?.Count ?? 0) 
            || (parameters.CountTypes?.Count ?? 0) != (parameters.CountValues?.Count ?? 0))
            throw new InvalidOperationException($"Parameters 'timeseriesCount', 'countType', 'countValue' must be of equal length. " +
                                                $"Got : timeseriesCount.Count = {timeSeriesCountsCount}, countType.Count = {parameters.CountTypes?.Count ?? 0}, countValue.Count = {parameters.CountValues?.Count ?? 0}.");

        var hs = new HashSet<AbstractTimeSeriesRange>(AbstractTimeSeriesRangeComparer.Instance);

        if (parameters.TimeSeries is { Count: > 0 })
        {
            for (int i = 0; i < parameters.TimeSeries.Count; i++)
            {
                hs.Add(new TimeSeriesRange
                {
                    Name = parameters.TimeSeries[i].ToString(),
                    From = parameters.From[i].IsEmpty
                        ? DateTime.MinValue
                        : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(parameters.From[i].Span, "from"),
                    To = parameters.To[i].IsEmpty
                        ? DateTime.MaxValue
                        : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(parameters.To[i].Span, "to")
                });
            }
        }

        if (parameters.TimeSeriesTimes is { Count: > 0 })
        {
            for (int i = 0; i < parameters.TimeSeriesTimes.Count; i++)
            {
                var timeValueUnit = (TimeValueUnit)Enum.Parse(typeof(TimeValueUnit), parameters.TimeUnits[i].Span);
                if (timeValueUnit == TimeValueUnit.None)
                    throw new InvalidOperationException(
                        $"Got unexpected {nameof(TimeValueUnit)} '{nameof(TimeValueUnit.None)}'. Only the following are supported: '{nameof(TimeValueUnit.Second)}' or '{nameof(TimeValueUnit.Month)}'.");

                if (int.TryParse(parameters.TimeValues[i].Span, out int res) == false)
                    throw new InvalidOperationException($"Could not parse timeseries time range value.");

                hs.Add(new TimeSeriesTimeRange
                {
                    Name = parameters.TimeSeriesTimes[i].ToString(),
                    Type = (TimeSeriesRangeType)Enum.Parse(typeof(TimeSeriesRangeType), parameters.TimeTypes[i].Span),
                    Time = timeValueUnit == TimeValueUnit.Second ? TimeValue.FromSeconds(res) : TimeValue.FromMonths(res)
                });
            }
        }

        if (parameters.TimeSeriesCounts is { Count: > 0 })
        {
            for (int i = 0; i < parameters.TimeSeriesCounts.Count; i++)
            {
                if (int.TryParse(parameters.CountValues[i].Span, out int res) == false)
                    throw new InvalidOperationException($"Could not parse timeseries count value.");

                hs.Add(new TimeSeriesCountRange
                {
                    Name = parameters.TimeSeriesCounts[i].ToString(),
                    Type = (TimeSeriesRangeType)Enum.Parse(typeof(TimeSeriesRangeType), parameters.CountTypes[i].Span),
                    Count = res
                });
            }
        }

        return hs;
    }

    public static async ValueTask<List<ReadOnlyMemory<char>>> GetIdsFromRequestBodyAsync(TOperationContext context, TRequestHandler requestHandler)
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

        public DocumentsTransaction ReadTransaction;
    }

    protected sealed class DocumentsResult
    {
        public IAsyncEnumerable<TDocumentType> DocumentsAsync { get; set; }

        public IEnumerable<TDocumentType> Documents { get; set; }

        public ShardedPagingContinuation ContinuationToken { get; set; }

        public string Etag { get; set; }

        public DocumentsTransaction ReadTransaction;
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

        public List<ReadOnlyMemory<char>> From;

        public List<ReadOnlyMemory<char>> To;

        public List<ReadOnlyMemory<char>> TimeTypes;

        public List<ReadOnlyMemory<char>> TimeValues;

        public List<ReadOnlyMemory<char>> TimeUnits;

        public List<ReadOnlyMemory<char>> CountTypes;

        public List<ReadOnlyMemory<char>> CountValues;

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
            From = RetrieveValues("from");
            To = RetrieveValues("to");
            TimeTypes = RetrieveValues("timeType");
            TimeValues = RetrieveValues("timeValue");
            TimeUnits = RetrieveValues("timeUnit");
            CountTypes = RetrieveValues("countType");
            CountValues = RetrieveValues("countValue");
        }

        protected override void OnValue(QueryStringEnumerable.EncodedNameValuePair pair)
        {
            var name = pair.EncodedName;

            switch (name.Length)
            {
                case 2:
                {
                    if (_isGet && IsMatch(name, IdQueryStringName))
                    {
                        Ids ??= new List<ReadOnlyMemory<char>>(1);
                        Ids.Add(pair.DecodeValue());
                        return;
                    }

                    if (IsMatch(name, ToQueryStringName))
                        AddForStringValues("to", pair.DecodeValue()); // optimize this
                    return;
                }
                case 4:
                {
                    if (IsMatch(name, FromQueryStringName))
                        AddForStringValues("from", pair.DecodeValue()); // optimize this
                    return;
                }
                case 6:
                {
                    if (IsMatch(name, TxModeQueryStringName))
                    {
                        if (TryGetEnumValue<TransactionMode>(pair.EncodedValue, out var value))
                            TxMode = value;
                    }

                    return;
                }
                case 7:
                {
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
                        AddForStringValues("counter", pair.DecodeValue()); // optimize this

                    return;
                }
                case 8:
                {
                    if (IsMatch(name, TimeTypeQueryStringName))
                    {
                        AddForStringValues("timeType", pair.DecodeValue()); // optimize this
                        return;
                    }

                    if (IsMatch(name, TimeUnitQueryStringName))
                        AddForStringValues("timeUnit", pair.DecodeValue()); // optimize this
                    return;
                }
                case 9:
                {
                    if (IsMatch(name, RevisionsQueryStringName))
                    {
                        Revisions ??= new List<ReadOnlyMemory<char>>(1);
                        Revisions.Add(pair.DecodeValue());
                        return;
                    }

                    if (IsMatch(name, TimeValueQueryStringName))
                    {
                        // optimize this
                        AddForStringValues("timeValue", pair.DecodeValue());
                        return;
                    }

                    if (IsMatch(name, CountTypeQueryStringName))
                        AddForStringValues("countType", pair.DecodeValue()); // optimize this

                    return;
                }
                case 10:
                {
                    if (IsMatch(name, CountValueQueryStringName))
                    {
                        AddForStringValues("countValue", pair.DecodeValue()); // optimize this
                        return;
                    }

                    if (IsMatch(name, TimeSeriesQueryStringName))
                    {
                        TimeSeries ??= new List<ReadOnlyMemory<char>>(1);

                        var value = pair.DecodeValue();
                        if (value.Span.Equals(AllTimeSeries.Span, StringComparison.Ordinal))
                            TimeSeriesHasAllTimeSeries = true;

                        TimeSeries.Add(value);
                    }

                    return;
                }
                case 12:
                {
                    if (IsMatch(name, MetadataOnlyQueryStringName))
                        MetadataOnly = GetBoolValue(name, pair.EncodedValue);
                    return;
                }
                case 14:
                {
                    if (IsMatch(name, TimeSeriesTimesQueryStringName))
                    {
                        TimeSeriesTimes ??= new List<ReadOnlyMemory<char>>(1);

                        var value = pair.DecodeValue();
                        if (value.Span.Equals(AllTimeSeries.Span, StringComparison.Ordinal))
                            TimeSeriesTimesHasAllTimeSeries = true;

                        TimeSeriesTimes.Add(value);
                    }

                    return;
                }
                case 15:
                {
                    if (IsMatch(name, RevisionsBeforeQueryStringName))
                    {
                        RevisionsBefore = pair.DecodeValue();
                        return;
                    }

                    if (IsMatch(name, TimeSeriesCountsQueryStringName))
                    {
                        TimeSeriesCounts ??= new List<ReadOnlyMemory<char>>(1);

                        var value = pair.DecodeValue();
                        if (value.Span.Equals(AllTimeSeries.Span, StringComparison.Ordinal))
                            TimeSeriesCountsHasAllTimeSeries = true;

                        TimeSeriesCounts.Add(value);
                    }

                    return;
                }
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
