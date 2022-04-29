using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Queries.Revisions;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal abstract class AbstractDocumentHandlerProcessorForGet<TRequestHandler, TOperationContext, TDocumentType> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    // ReSharper disable once StaticMemberInGenericType
    private static readonly (long, long) NoResults = (-1, -1);

    private readonly HttpMethod _method;

    protected readonly List<IDisposable> Disposables = new();

    protected AbstractDocumentHandlerProcessorForGet(HttpMethod method, [NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
    {
        if (method != HttpMethod.Get && method != HttpMethod.Post)
            throw new InvalidOperationException($"The processor is supposed to handle GET and POST methods while '{method}' was specified");
        
        _method = method;
    }

    protected abstract bool SupportsShowingRequestInTrafficWatch { get; }

    protected abstract bool SupportsHandlingOfMissingIncludes { get; }

    protected abstract CancellationToken CancellationToken { get; }

    protected abstract void Initialize(TOperationContext jsonOperationContext);

    public override async ValueTask ExecuteAsync()
    {
        var metadataOnly = RequestHandler.GetBoolValueQueryString("metadataOnly", required: false) ?? false;
        var includePaths = RequestHandler.GetStringValuesQueryString("include", required: false);

        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            var sw = Stopwatch.StartNew();

            StringValues ids;

            if (_method == HttpMethod.Get)
                ids = RequestHandler.GetStringValuesQueryString("id", required: false);
            else if (_method == HttpMethod.Post)
                ids = await GetIdsFromRequestBody(context);
            else
                throw new NotSupportedException($"Unhandled method type: {_method}");

            if (SupportsShowingRequestInTrafficWatch && TrafficWatchManager.HasRegisteredClients)
                RequestHandler.AddStringToHttpContext(ids.ToString(), TrafficWatchChangeType.Documents);

            Initialize(context);

            (long NumberOfResults, long TotalDocumentsSizeInBytes) responseWriteStats;
            int pageSize;
            string actionName;

            if (ids.Count > 0)
            {
                pageSize = ids.Count;
                actionName = nameof(GetDocumentsByIdAsync);

                var etag = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);

                // includes
                var counters = RequestHandler.GetStringValuesQueryString("counter", required: false);
                var revisions = GetRevisionsToInclude();
                var timeSeries = GetTimeSeriesToInclude();
                var compareExchangeValues = RequestHandler.GetStringValuesQueryString("cmpxchg", required: false);

                responseWriteStats = await GetDocumentsByIdAsync(context, ids, includePaths, revisions, counters, timeSeries, compareExchangeValues, metadataOnly, etag);
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

                responseWriteStats = await GetDocumentsAsync(context, etag, startsWithParams, metadataOnly, changeVector);
            }

            if (responseWriteStats != NoResults)
            {
                AddPagingPerformanceHint(PagingOperationType.Documents, actionName, HttpContext.Request.QueryString.Value, responseWriteStats.NumberOfResults,
                    pageSize, sw.ElapsedMilliseconds, responseWriteStats.TotalDocumentsSizeInBytes);
            }
        }
    }

    protected async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> GetDocumentsByIdAsync(TOperationContext context, StringValues ids, StringValues includePaths, RevisionIncludeField revisions,
        StringValues counters, HashSet<AbstractTimeSeriesRange> timeSeries, StringValues compareExchangeValues, bool metadataOnly, string etag)
    {
        var result = await GetDocumentsByIdImplAsync(context, ids, includePaths, revisions, counters, timeSeries, compareExchangeValues, metadataOnly, etag);

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

        if (SupportsHandlingOfMissingIncludes)
            await HandleMissingIncludes(context, result, metadataOnly);

        return await WriteDocumentsByIdResultAsync(context, metadataOnly, result);
    }

    private async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsByIdResultAsync(
        TOperationContext context, bool metadataOnly, DocumentsByIdResult<TDocumentType> result)
    {
        long numberOfResults;
        long totalDocumentsSizeInBytes;
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            (numberOfResults, totalDocumentsSizeInBytes) = await WriteDocumentsAsync(writer, context, nameof(GetDocumentsResult.Results), result.Documents, metadataOnly, CancellationToken);
            
            writer.WriteComma();

            string includesPropertyName = nameof(GetDocumentsResult.Includes);

            if (result.Includes.Count > 0)
            {
                await WriteIncludesAsync(writer, context, includesPropertyName, result.Includes, CancellationToken);
            }
            else
            {
                writer.WritePropertyName(includesPropertyName);
                writer.WriteStartObject();
                writer.WriteEndObject();
            }

            if (result.CounterIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.CounterIncludes));

                await writer.WriteCountersAsync(result.CounterIncludes, CancellationToken);
            }

            if (result.TimeSeriesIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.TimeSeriesIncludes));
                await writer.WriteTimeSeriesAsync(result.TimeSeriesIncludes, CancellationToken);
            }

            if (result.RevisionsChangeVectorIncludes?.Count > 0 || result.IdByRevisionsByDateTimeIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.RevisionIncludes));
                writer.WriteStartArray();
                await writer.WriteRevisionIncludes(context: context, revisionsByChangeVector: result.RevisionsChangeVectorIncludes, revisionsByDateTime: result.IdByRevisionsByDateTimeIncludes, CancellationToken);
                writer.WriteEndArray();
            }

            if (result.CompareExchangeIncludes?.Count > 0)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(GetDocumentsResult.CompareExchangeValueIncludes));
                await writer.WriteCompareExchangeValuesAsync(result.CompareExchangeIncludes, CancellationToken);
            }

            writer.WriteEndObject();
        }
        return (numberOfResults, totalDocumentsSizeInBytes);
    }

    protected abstract ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteDocumentsAsync(AsyncBlittableJsonTextWriter writer, TOperationContext context, string propertyName,
        List<TDocumentType> documentsToWrite, bool metadataOnly, CancellationToken token);

    protected abstract ValueTask WriteIncludesAsync(AsyncBlittableJsonTextWriter writer, TOperationContext context, string propertyName,
        List<TDocumentType> includes, CancellationToken token);

    protected abstract ValueTask<DocumentsByIdResult<TDocumentType>> GetDocumentsByIdImplAsync(TOperationContext context, StringValues ids,
        StringValues includePaths, RevisionIncludeField revisions, StringValues counters, HashSet<AbstractTimeSeriesRange> timeSeries, StringValues compareExchangeValues, bool metadataOnly, string etag);

    protected abstract ValueTask HandleMissingIncludes(TOperationContext context, DocumentsByIdResult<TDocumentType> result, bool metadataOnly);

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

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("Results");

            if (result.DocumentsAsync != null)
                (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentsAsync(context, result.DocumentsAsync, metadataOnly, CancellationToken);
            else
                (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentsAsync(context, result.Documents, metadataOnly, CancellationToken);

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

    protected abstract void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration, long totalDocumentsSizeInBytes);

    private async Task<StringValues> GetIdsFromRequestBody(TOperationContext context)
    {
        var docs = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "docs");
        if (docs.TryGet("Ids", out BlittableJsonReaderArray array) == false)
            Web.RequestHandler.ThrowRequiredPropertyNameInRequest("Ids");

        var idsAsStrings = new string[array.Length];

        for (int i = 0; i < array.Length; i++)
        {
            idsAsStrings[i] = array.GetStringByIndex(i);
        }

        return new StringValues(idsAsStrings);
    }

    private RevisionIncludeField GetRevisionsToInclude()
    {
        var revisionsByChangeVectors = RequestHandler.GetStringValuesQueryString("revisions", required: false);
        var revisionByDateTimeBefore = RequestHandler.GetStringValuesQueryString("revisionsBefore", required: false);

        if (revisionsByChangeVectors.Count == 0 && revisionByDateTimeBefore.Count == 0)
            return null;

        var rif = new RevisionIncludeField();

        if (DateTime.TryParseExact(revisionByDateTimeBefore.ToString(), DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dateTime))
            rif.RevisionsBeforeDateTime = dateTime.ToUniversalTime();

        foreach (var changeVector in revisionsByChangeVectors)
            rif.RevisionsChangeVectorsPaths.Add(changeVector);

        return rif;
    }

    private HashSet<AbstractTimeSeriesRange> GetTimeSeriesToInclude()
    {
        var timeSeriesNames = RequestHandler.GetStringValuesQueryString("timeseries", required: false);
        var timeSeriesTimeNames = RequestHandler.GetStringValuesQueryString("timeseriestime", required: false);
        var timeSeriesCountNames = RequestHandler.GetStringValuesQueryString("timeseriescount", required: false);
        if (timeSeriesNames.Count == 0 && timeSeriesTimeNames.Count == 0 && timeSeriesCountNames.Count == 0)
            return null;

        if (timeSeriesNames.Count > 1 && timeSeriesNames.Contains(Constants.TimeSeries.All))
            throw new InvalidOperationException($"Cannot have more than one include on '{Constants.TimeSeries.All}'.");
        if (timeSeriesTimeNames.Count > 1 && timeSeriesTimeNames.Contains(Constants.TimeSeries.All))
            throw new InvalidOperationException($"Cannot have more than one include on '{Constants.TimeSeries.All}'.");
        if (timeSeriesCountNames.Count > 1 && timeSeriesCountNames.Contains(Constants.TimeSeries.All))
            throw new InvalidOperationException($"Cannot have more than one include on '{Constants.TimeSeries.All}'.");

        var fromList = RequestHandler.GetStringValuesQueryString("from", required: false);
        var toList = RequestHandler.GetStringValuesQueryString("to", required: false);
        if (timeSeriesNames.Count != fromList.Count || fromList.Count != toList.Count)
            throw new InvalidOperationException("Parameters 'timeseriesNames', 'fromList' and 'toList' must be of equal length. " +
                                                $"Got : timeseriesNames.Count = {timeSeriesNames.Count}, fromList.Count = {fromList.Count}, toList.Count = {toList.Count}.");

        var timeTypeList = RequestHandler.GetStringValuesQueryString("timeType", required: false);
        var timeValueList = RequestHandler.GetStringValuesQueryString("timeValue", required: false);
        var timeUnitList = RequestHandler.GetStringValuesQueryString("timeUnit", required: false);
        if (timeSeriesTimeNames.Count != timeTypeList.Count || timeTypeList.Count != timeValueList.Count || timeValueList.Count != timeUnitList.Count)
            throw new InvalidOperationException($"Parameters '{nameof(timeSeriesTimeNames)}', '{nameof(timeTypeList)}', '{nameof(timeValueList)}' and '{nameof(timeUnitList)}' must be of equal length. " +
                                                $"Got : {nameof(timeSeriesTimeNames)}.Count = {timeSeriesTimeNames.Count}, {nameof(timeTypeList)}.Count = {timeTypeList.Count}, {nameof(timeValueList)}.Count = {timeValueList.Count}, {nameof(timeUnitList)}.Count = {timeUnitList.Count}.");

        var countTypeList = RequestHandler.GetStringValuesQueryString("countType", required: false);
        var countValueList = RequestHandler.GetStringValuesQueryString("countValue", required: false);
        if (timeSeriesCountNames.Count != countTypeList.Count || countTypeList.Count != countValueList.Count)
            throw new InvalidOperationException($"Parameters '{nameof(timeSeriesCountNames)}', '{nameof(countTypeList)}', '{nameof(countValueList)}' must be of equal length. " +
                                                $"Got : {nameof(timeSeriesCountNames)}.Count = {timeSeriesCountNames.Count}, {nameof(countTypeList)}.Count = {countTypeList.Count}, {nameof(countValueList)}.Count = {countValueList.Count}.");

        var hs = new HashSet<AbstractTimeSeriesRange>(AbstractTimeSeriesRangeComparer.Instance);

        for (int i = 0; i < timeSeriesNames.Count; i++)
        {
            hs.Add(new TimeSeriesRange
            {
                Name = timeSeriesNames[i],
                From = string.IsNullOrEmpty(fromList[i])
                    ? DateTime.MinValue
                    : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(fromList[i], "from"),
                To = string.IsNullOrEmpty(toList[i])
                    ? DateTime.MaxValue
                    : TimeSeriesHandlerProcessorForGetTimeSeries.ParseDate(toList[i], "to")
            });
        }

        for (int i = 0; i < timeSeriesTimeNames.Count; i++)
        {
            var timeValueUnit = (TimeValueUnit)Enum.Parse(typeof(TimeValueUnit), timeUnitList[i]);
            if (timeValueUnit == TimeValueUnit.None)
                throw new InvalidOperationException($"Got unexpected {nameof(TimeValueUnit)} '{nameof(TimeValueUnit.None)}'. Only the following are supported: '{nameof(TimeValueUnit.Second)}' or '{nameof(TimeValueUnit.Month)}'.");

            if (int.TryParse(timeValueList[i], out int res) == false)
                throw new InvalidOperationException($"Could not parse timeseries time range value.");

            hs.Add(new TimeSeriesTimeRange
            {
                Name = timeSeriesTimeNames[i],
                Type = (TimeSeriesRangeType)Enum.Parse(typeof(TimeSeriesRangeType), timeTypeList[i]),
                Time = timeValueUnit == TimeValueUnit.Second ? TimeValue.FromSeconds(res) : TimeValue.FromMonths(res)
            });
        }

        for (int i = 0; i < timeSeriesCountNames.Count; i++)
        {
            if (int.TryParse(countValueList[i], out int res) == false)
                throw new InvalidOperationException($"Could not parse timeseries count value.");

            hs.Add(new TimeSeriesCountRange
            {
                Name = timeSeriesCountNames[i],
                Type = (TimeSeriesRangeType)Enum.Parse(typeof(TimeSeriesRangeType), countTypeList[i]),
                Count = res
            });
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

    protected class DocumentsByIdResult<T>
    {
        public List<T> Documents { get; set; }

        public List<T> Includes { get; set; }

        public Dictionary<string, Document> RevisionsChangeVectorIncludes { get; set; }

        public Dictionary<string, Dictionary<DateTime, Document>> IdByRevisionsByDateTimeIncludes { get; set; }

        public Dictionary<string, List<CounterDetail>> CounterIncludes { get; set; }

        public Dictionary<string, Dictionary<string, List<TimeSeriesRangeResult>>> TimeSeriesIncludes { get; set; }

        public Dictionary<string, CompareExchangeValue<BlittableJsonReaderObject>> CompareExchangeIncludes { get; set; }

        public HashSet<string> MissingIncludes { get; set; }

        public string Etag { get; set; }

        public HttpStatusCode StatusCode { get; set; } = HttpStatusCode.OK;
    }

    protected class DocumentsResult
    {
        public IAsyncEnumerable<Document> DocumentsAsync { get; set; }

        public IEnumerable<Document> Documents { get; set; }

        public ShardedPagingContinuation ContinuationToken { get; set; }

        public string Etag { get; set; }
    }

    protected class StartsWithParams
    {
        public string IdPrefix { get; set; }

        public string Matches { get; set; }
        
        public string Exclude { get; set; }
        
        public string StartAfterId { get; set; }
    }
}
