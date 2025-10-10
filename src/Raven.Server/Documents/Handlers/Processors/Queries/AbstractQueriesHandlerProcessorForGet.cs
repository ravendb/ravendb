using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.WebUtilities;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Queries;

internal abstract class AbstractQueriesHandlerProcessorForGet<TRequestHandler, TOperationContext, TQueryContext, TQueryResult, TQueryResultsContainer> : AbstractQueriesHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TQueryResultsContainer : QueryResultServerSide<TQueryResult>
    where TQueryContext : IDisposable
{
    private readonly QueryStringParameters _parameters;
    public bool AddSpatialProperties => _parameters.AddSpatialProperties;

    protected AbstractQueriesHandlerProcessorForGet([NotNull] TRequestHandler requestHandler, QueryMetadataCache queryMetadataCache, HttpMethod method) : base(requestHandler, queryMetadataCache)
    {
        QueryMethod = method;
        _parameters = QueryStringParameters.Create(HttpContext.Request);
    }

    internal abstract IDisposable AllocateContextForQueryOperation(out TQueryContext queryContext, out TOperationContext context);

    private ValueTask HandleDebugAsync(IndexQueryServerSide query, TQueryContext queryContext, TOperationContext context, QueryStringParameters parameters, long? existingResultEtag, OperationCancelToken token)
    {
        var debug = parameters.Debug;
        if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
        {
            var ignoreLimit = parameters.IgnoreLimit;
            return IndexEntriesAsync(queryContext, context, query, existingResultEtag, ignoreLimit, token);
        }

        if (string.Equals(debug, "explain", StringComparison.OrdinalIgnoreCase))
        {
            return ExplainAsync(queryContext, query, token);
        }

        if (string.Equals(debug, "serverSideQuery", StringComparison.OrdinalIgnoreCase))
        {
            return ServerSideQueryAsync(context, query);
        }

        return ValueTask.FromException(new NotSupportedException($"Not supported query debug operation: '{debug}'"));
    }

    protected abstract Task<IndexEntriesQueryResult> GetIndexEntriesAsync(TQueryContext queryContext, TOperationContext context, IndexQueryServerSide query, long? existingResultEtag, bool ignoreLimit, OperationCancelToken token);

    private async ValueTask IndexEntriesAsync(TQueryContext queryContext, TOperationContext context, IndexQueryServerSide query, long? existingResultEtag, bool ignoreLimit, OperationCancelToken token)
    {
        var result = await GetIndexEntriesAsync(queryContext, context, query, existingResultEtag, ignoreLimit, token);

        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
        {
            var writeIndexEntriesQueryResultsTask = writer.WriteIndexEntriesQueryResultAsync(context, result, token.Token);
            if (writeIndexEntriesQueryResultsTask.IsCompletedSuccessfully == false)
                await writeIndexEntriesQueryResultsTask;
        }
    }

    protected abstract ValueTask ExplainAsync(TQueryContext queryContext, IndexQueryServerSide query, OperationCancelToken token);

    protected abstract Task<FacetedQueryResult> GetFacetedQueryResultAsync(IndexQueryServerSide query, TQueryContext queryContext, long? existingResultEtag, OperationCancelToken token);

    protected abstract Task<SuggestionQueryResult> GetSuggestionQueryResultAsync(IndexQueryServerSide query, TQueryContext queryContext, long? existingResultEtag, OperationCancelToken token);

    protected abstract Task<TQueryResultsContainer> GetQueryResultsAsync(IndexQueryServerSide query, TQueryContext queryContext, long? existingResultEtag,
        bool metadataOnly,
        OperationCancelToken token);

    protected override HttpMethod QueryMethod { get; }

    internal async ValueTask<T> ExecuteWithExceptionHandling<T>(ValueTask<T> task, RequestTimeTracker tracker)
    {
        try
        {
            return await task;
        }
        catch (Exception e)
        {
            if (tracker.Query == null)
            {
                string errorMessage;
                if (e is EndOfStreamException || e is ArgumentException)
                {
                    errorMessage = $"Failed: {e.Message}";
                }
                else
                {
                    errorMessage = $"Failed: {HttpContext.Request.Path.Value} {e}";
                }

                tracker.Query = errorMessage;

                if (TrafficWatchManager.HasRegisteredClients)
                    RequestHandler.AddStringToHttpContext(errorMessage, TrafficWatchChangeType.Queries);
            }

            throw;
        }
    }
    
    internal async ValueTask ExecuteWithExceptionHandling(ValueTask task, RequestTimeTracker tracker)
    {
        try
        {
            await task;
        }
        catch (Exception e)
        {
            if (tracker.Query == null)
            {
                string errorMessage;
                if (e is EndOfStreamException || e is ArgumentException)
                {
                    errorMessage = $"Failed: {e.Message}";
                }
                else
                {
                    errorMessage = $"Failed: {HttpContext.Request.Path.Value} {e}";
                }

                tracker.Query = errorMessage;

                if (TrafficWatchManager.HasRegisteredClients)
                    RequestHandler.AddStringToHttpContext(errorMessage, TrafficWatchChangeType.Queries);
            }

            throw;
        }
    }

    public ValueTask ExecuteQuery(TQueryContext queryContext, TOperationContext context, RequestTimeTracker tracker, IndexQueryServerSide indexQuery, OperationCancelToken token)
    {
        indexQuery.Diagnostics = _parameters.Diagnostics ? new List<string>() : null;
        indexQuery.AddTimeSeriesNames = _parameters.AddTimeSeriesNames;
        indexQuery.DisableAutoIndexCreation = _parameters.DisableAutoIndexCreation;

        if (RequestHandler.HttpContext.Request.IsFromOrchestrator())
            indexQuery.ReturnOptions = IndexQueryServerSide.QueryResultReturnOptions.CreateForSharding(indexQuery);

        AssertIndexQuery(indexQuery);

        var existingResultEtag = RequestHandler.GetLongFromHeaders(Constants.Headers.IfNoneMatch);

        EnsureQueryContextInitialized(queryContext, indexQuery);

        return ExecuteWithExceptionHandling(ProcessQueryAsync(), tracker);


        ValueTask ProcessQueryAsync()
        {
            if (string.IsNullOrWhiteSpace(_parameters.Debug) == false)
            {
                return HandleDebugAsync(indexQuery, queryContext, context, _parameters, existingResultEtag, token);
            }

            if (TrafficWatchManager.HasRegisteredClients)
                RequestHandler.TrafficWatchQuery(indexQuery);

            if (indexQuery.Metadata.HasFacet)
            {
                return HandleFacetedQueryAsync(indexQuery, queryContext, context, existingResultEtag, token);
            }

            if (indexQuery.Metadata.HasSuggest)
            {
                return HandleSuggestQueryAsync(indexQuery, queryContext, context, existingResultEtag, token);
            }

            return HandleIndexQueryAsync(indexQuery, queryContext, existingResultEtag, _parameters, token, context);
        }
    }

    private async ValueTask HandleIndexQueryAsync(IndexQueryServerSide indexQuery, TQueryContext queryContext, long? existingResultEtag, QueryStringParameters parameters,
        OperationCancelToken token, TOperationContext context)
    {
        TQueryResultsContainer result = null;
        try
        {
            result = await GetQueryResultsAsync(indexQuery, queryContext, existingResultEtag, parameters.MetadataOnly, token);
        }
        catch (IndexDoesNotExistException)
        {
            result?.Dispose();
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }
        catch (Exception)
        {
            result?.Dispose();
            throw;
        }

        using (result)
        {
            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

            long numberOfResults;
            long totalDocumentsSizeInBytes;
            var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token);
            try
            {
                result.Timings = indexQuery.Timings?.ToTimings();
                var writeResultsTask = writer.WriteDocumentQueryResultAsync(context, result, parameters.MetadataOnly,
                    WriteAdditionalData(indexQuery, parameters.IncludeServerSideQuery), token.Token);
                (numberOfResults, totalDocumentsSizeInBytes) = writeResultsTask.IsCompletedSuccessfully ? writeResultsTask.Result : await writeResultsTask;
            }
            finally
            {
                var disposeAsync = writer.DisposeAsync();
                if (disposeAsync.IsCompletedSuccessfully == false)
                    await disposeAsync;
            }


            QueryMetadataCache.MaybeAddToCache(indexQuery.Metadata, result.IndexName);

            if (RequestHandler.ShouldAddPagingPerformanceHint(numberOfResults))
            {
                RequestHandler.AddPagingPerformanceHint(PagingOperationType.Queries, $"Query ({result.IndexName})",
                    $"{indexQuery.Metadata.QueryText}\n{indexQuery.QueryParameters}", numberOfResults, indexQuery.PageSize, result.DurationInMs,
                    totalDocumentsSizeInBytes);
            }

            AddQueryTimingsToTrafficWatch(indexQuery);
        }
    }

    public override ValueTask ExecuteAsync() => throw new NotImplementedException();


    protected virtual void AssertIndexQuery(IndexQueryServerSide indexQuery)
    {
    }

    protected virtual void EnsureQueryContextInitialized(TQueryContext queryContext, IndexQueryServerSide indexQuery)
    {
    }

    private static Action<AbstractBlittableJsonTextWriter> WriteAdditionalData(IndexQueryServerSide indexQuery, bool shouldReturnServerSideQuery)
    {
        if (indexQuery.Diagnostics == null && shouldReturnServerSideQuery == false)
            return null;

        return w =>
        {
            if (shouldReturnServerSideQuery)
            {
                w.WriteComma();
                w.WritePropertyName(nameof(indexQuery.ServerSideQuery));
                w.WriteString(indexQuery.ServerSideQuery);
            }

            if (indexQuery.Diagnostics != null)
            {
                w.WriteComma();
                w.WriteArray(nameof(indexQuery.Diagnostics), indexQuery.Diagnostics);
            }
        };
    }

    private async ValueTask ServerSideQueryAsync(TOperationContext context, IndexQueryServerSide indexQuery)
    {
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(indexQuery.ServerSideQuery));
            writer.WriteString(indexQuery.ServerSideQuery);

            writer.WriteEndObject();
        }
    }

    private async ValueTask HandleSuggestQueryAsync(IndexQueryServerSide query, TQueryContext queryContext, TOperationContext operationContext, long? existingResultEtag, OperationCancelToken token)
    {
        var result = await GetSuggestionQueryResultAsync(query, queryContext, existingResultEtag, token);

        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        long numberOfResults;
        long totalDocumentsSizeInBytes;
        await using (var writer = new AsyncBlittableJsonTextWriter(operationContext, RequestHandler.ResponseBodyStream(), token.Token))
        {
            var writeSuggestionQueryResultTask = writer.WriteSuggestionQueryResultAsync(operationContext, result, token.Token);
            (numberOfResults, totalDocumentsSizeInBytes) = writeSuggestionQueryResultTask.IsCompletedSuccessfully
                ? writeSuggestionQueryResultTask.Result
                : await writeSuggestionQueryResultTask;
        }

        if (RequestHandler.ShouldAddPagingPerformanceHint(numberOfResults))
            RequestHandler.AddPagingPerformanceHint(PagingOperationType.Queries, $"SuggestQuery ({result.IndexName})", query.Query, numberOfResults, query.PageSize, result.DurationInMs, totalDocumentsSizeInBytes);
    }

    private async ValueTask HandleFacetedQueryAsync(IndexQueryServerSide query, TQueryContext queryContext, TOperationContext operationContext, long? existingResultEtag, OperationCancelToken token)
    {
        var result = await GetFacetedQueryResultAsync(query, queryContext, existingResultEtag, token);

        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        long numberOfResults;
        await using (var writer = new AsyncBlittableJsonTextWriter(operationContext, RequestHandler.ResponseBodyStream(), token.Token))
        {
            result.Timings = query.Timings?.ToTimings();
            numberOfResults = await writer.WriteFacetedQueryResultAsync(operationContext, result, token.Token);
        }

        QueryMetadataCache.MaybeAddToCache(query.Metadata, result.IndexName);

        if (RequestHandler.ShouldAddPagingPerformanceHint(numberOfResults))
            RequestHandler.AddPagingPerformanceHint(PagingOperationType.Queries, $"FacetedQuery ({result.IndexName})", $"{query.Metadata.QueryText}\n{query.QueryParameters}", numberOfResults, query.PageSize, result.DurationInMs, -1);

        AddQueryTimingsToTrafficWatch(query);
    }

    private void AddQueryTimingsToTrafficWatch(IndexQueryServerSide indexQuery)
    {
        if (TrafficWatchManager.HasRegisteredClients && indexQuery.Timings != null)
            HttpContext.Items[nameof(QueryTimings)] = indexQuery.Timings.ToTimings();
    }

    private sealed class QueryStringParameters : AbstractQueryStringParameters
    {
        public bool MetadataOnly;

        public bool AddSpatialProperties;

        public bool IncludeServerSideQuery;

        public bool Diagnostics;

        public bool AddTimeSeriesNames;

        public bool DisableAutoIndexCreation;

        public string Debug;

        public bool IgnoreLimit;

        private QueryStringParameters([NotNull] HttpRequest httpRequest)
            : base(httpRequest)
        {
        }

        protected override void OnFinalize()
        {
        }

        protected override void OnValue(QueryStringEnumerable.EncodedNameValuePair pair)
        {
            var name = pair.EncodedName;

            switch (name.Length)
            {
                case 5:
                {
                    if (IsMatch(name, DebugQueryStringName))
                        Debug = pair.DecodeValue().ToString();
                    return;
                }
                case 11:
                {
                    if (IsMatch(name, IgnoreLimitQueryStringName))
                    {
                        IgnoreLimit = GetBoolValue(name, pair.EncodedValue);
                        return;
                    }

                    if (IsMatch(name, DiagnosticsQueryStringName))
                        Diagnostics = GetBoolValue(name, pair.EncodedValue);

                    return;
                }
                case 12:
                {
                    if (IsMatch(name, MetadataOnlyQueryStringName))
                        MetadataOnly = GetBoolValue(name, pair.EncodedValue);
                    return;
                }
                case 18:
                {
                    if (IsMatch(name, AddTimeSeriesNamesQueryStringName))
                        AddTimeSeriesNames = GetBoolValue(name, pair.EncodedValue);
                    return;
                }
                case 20:
                {
                    if (IsMatch(name, AddSpatialPropertiesQueryStringName))
                        AddSpatialProperties = GetBoolValue(name, pair.EncodedValue);
                    return;
                }
                case 22:
                {
                    if (IsMatch(name, IncludeServerSideQueryQueryStringName))
                        IncludeServerSideQuery = GetBoolValue(name, pair.EncodedValue);
                    return;
                }
                case 24:
                {
                    if (IsMatch(name, DisableAutoIndexCreationQueryStringName))
                        DisableAutoIndexCreation = GetBoolValue(name, pair.EncodedValue);
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
