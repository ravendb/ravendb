using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
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
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.TrafficWatch;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Queries;

internal abstract class AbstractQueriesHandlerProcessorForGet<TRequestHandler, TOperationContext, TQueryContext, TQueryResult> : AbstractQueriesHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TQueryContext : IDisposable
{
    protected AbstractQueriesHandlerProcessorForGet([NotNull] TRequestHandler requestHandler, QueryMetadataCache queryMetadataCache, HttpMethod method) : base(requestHandler, queryMetadataCache)
    {
        QueryMethod = method;
    }

    protected abstract IDisposable AllocateContextForQueryOperation(out TQueryContext queryContext, out TOperationContext context);

    private async ValueTask HandleDebugAsync(IndexQueryServerSide query, TQueryContext queryContext, TOperationContext context, string debug, long? existingResultEtag, OperationCancelToken token)
    {
        if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
        {
            var ignoreLimit = RequestHandler.GetBoolValueQueryString("ignoreLimit", required: false) ?? false;
            await IndexEntriesAsync(queryContext, context, query, existingResultEtag, ignoreLimit, token);
            return;
        }

        if (string.Equals(debug, "explain", StringComparison.OrdinalIgnoreCase))
        {
            await ExplainAsync(queryContext, query, token);
            return;
        }

        if (string.Equals(debug, "serverSideQuery", StringComparison.OrdinalIgnoreCase))
        {
            await ServerSideQueryAsync(context, query);
            return;
        }

        throw new NotSupportedException($"Not supported query debug operation: '{debug}'");
    }

    protected abstract ValueTask<IndexEntriesQueryResult> GetIndexEntriesAsync(TQueryContext queryContext, TOperationContext context, IndexQueryServerSide query, long? existingResultEtag, bool ignoreLimit, OperationCancelToken token);

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
            await writer.WriteIndexEntriesQueryResultAsync(context, result, token.Token);
        }
    }

    protected abstract ValueTask ExplainAsync(TQueryContext queryContext, IndexQueryServerSide query, OperationCancelToken token);

    protected abstract ValueTask<FacetedQueryResult> GetFacetedQueryResultAsync(IndexQueryServerSide query, TQueryContext queryContext, long? existingResultEtag, OperationCancelToken token);

    protected abstract ValueTask<SuggestionQueryResult> GetSuggestionQueryResultAsync(IndexQueryServerSide query, TQueryContext queryContext, long? existingResultEtag, OperationCancelToken token);

    protected abstract ValueTask<QueryResultServerSide<TQueryResult>> GetQueryResultsAsync(IndexQueryServerSide query, TQueryContext queryContext, long? existingResultEtag,
        bool metadataOnly,
        OperationCancelToken token);

    protected override HttpMethod QueryMethod { get; }

    public override async ValueTask ExecuteAsync()
    {
        using (var tracker = CreateRequestTimeTracker())
        {
            try
            {
                using (var token = RequestHandler.CreateHttpRequestBoundTimeLimitedOperationTokenForQuery())
                using (AllocateContextForQueryOperation(out var queryContext, out var context))
                {
                    var addSpatialProperties = RequestHandler.GetBoolValueQueryString("addSpatialProperties", required: false) ?? false;
                    var metadataOnly = RequestHandler.GetBoolValueQueryString("metadataOnly", required: false) ?? false;
                    var shouldReturnServerSideQuery = RequestHandler.GetBoolValueQueryString("includeServerSideQuery", required: false) ?? false;

                    var indexQuery = await GetIndexQueryAsync(context, QueryMethod, tracker, addSpatialProperties);

                    indexQuery.Diagnostics = RequestHandler.GetBoolValueQueryString("diagnostics", required: false) ?? false ? new List<string>() : null;
                    indexQuery.AddTimeSeriesNames = RequestHandler.GetBoolValueQueryString("addTimeSeriesNames", false) ?? false;
                    indexQuery.DisableAutoIndexCreation = RequestHandler.GetBoolValueQueryString("disableAutoIndexCreation", false) ?? false;

                    if (RequestHandler.HttpContext.Request.IsFromOrchestrator())
                        indexQuery.ReturnOptions = IndexQueryServerSide.QueryResultReturnOptions.CreateForSharding(indexQuery);

                    AssertIndexQuery(indexQuery);

                    var existingResultEtag = RequestHandler.GetLongFromHeaders(Constants.Headers.IfNoneMatch);

                    var debug = RequestHandler.GetStringQueryString("debug", required: false);

                    EnsureQueryContextInitialized(queryContext, indexQuery);

                    if (string.IsNullOrWhiteSpace(debug) == false)
                    {
                        await HandleDebugAsync(indexQuery, queryContext, context, debug, existingResultEtag, token);
                        return;
                    }

                    if (TrafficWatchManager.HasRegisteredClients)
                        RequestHandler.TrafficWatchQuery(indexQuery);

                    if (indexQuery.Metadata.HasFacet)
                    {
                        await HandleFacetedQueryAsync(indexQuery, queryContext, context, existingResultEtag, token);
                        return;
                    }

                    if (indexQuery.Metadata.HasSuggest)
                    {
                        await HandleSuggestQueryAsync(indexQuery, queryContext, context, existingResultEtag, token);
                        return;
                    }

                    QueryResultServerSide<TQueryResult> result = null;
                    try
                    {
                        result = await GetQueryResultsAsync(indexQuery, queryContext, existingResultEtag, metadataOnly, token);
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
                        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
                        {
                            result.Timings = indexQuery.Timings?.ToTimings();

                            (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentQueryResultAsync(context, result, metadataOnly,
                                WriteAdditionalData(indexQuery, shouldReturnServerSideQuery), token.Token);
                            await writer.MaybeOuterFlushAsync();
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
    }

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
            (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteSuggestionQueryResultAsync(operationContext, result, token.Token);
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
}
