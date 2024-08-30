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
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.TrafficWatch;
using Raven.Server.Web;
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

    private async ValueTask HandleDebugAsync(IndexQueryServerSide query, TQueryContext queryContext, TOperationContext context, QueryStringParameters parameters, long? existingResultEtag, OperationCancelToken token)
    {
        var debug = parameters.Debug;
        if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
        {
            var ignoreLimit = parameters.IgnoreLimit;
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
                    var parameters = QueryStringParameters.Create(HttpContext.Request);
                    var indexQuery = await GetIndexQueryAsync(context, QueryMethod, tracker, parameters.AddSpatialProperties);

                    indexQuery.Diagnostics = parameters.Diagnostics ? new List<string>() : null;
                    indexQuery.AddTimeSeriesNames = parameters.AddTimeSeriesNames;
                    indexQuery.DisableAutoIndexCreation = parameters.DisableAutoIndexCreation;

                    if (RequestHandler.HttpContext.Request.IsFromOrchestrator())
                        indexQuery.ReturnOptions = IndexQueryServerSide.QueryResultReturnOptions.CreateForSharding(indexQuery);

                    AssertIndexQuery(indexQuery);

                    var existingResultEtag = RequestHandler.GetLongFromHeaders(Constants.Headers.IfNoneMatch);

                    EnsureQueryContextInitialized(queryContext, indexQuery);

                    if (string.IsNullOrWhiteSpace(parameters.Debug) == false)
                    {
                        await HandleDebugAsync(indexQuery, queryContext, context, parameters, existingResultEtag, token);
                        
                        tracker.Dispose();

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
                        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
                        {
                            result.Timings = indexQuery.Timings?.ToTimings();

                            (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentQueryResultAsync(context, result, parameters.MetadataOnly,
                                WriteAdditionalData(indexQuery, parameters.IncludeServerSideQuery), token.Token);
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

                    tracker.Dispose();
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

            if (IsMatch(name, MetadataOnlyQueryStringName))
            {
                MetadataOnly = GetBoolValue(name, pair.EncodedValue);
                return;
            }

            if (IsMatch(name, AddSpatialPropertiesQueryStringName))
            {
                AddSpatialProperties = GetBoolValue(name, pair.EncodedValue);
                return;
            }

            if (IsMatch(name, IncludeServerSideQueryQueryStringName))
            {
                IncludeServerSideQuery = GetBoolValue(name, pair.EncodedValue);
                return;
            }

            if (IsMatch(name, DiagnosticsQueryStringName))
            {
                Diagnostics = GetBoolValue(name, pair.EncodedValue);
                return;
            }

            if (IsMatch(name, AddTimeSeriesNamesQueryStringName))
            {
                AddTimeSeriesNames = GetBoolValue(name, pair.EncodedValue);
                return;
            }

            if (IsMatch(name, DisableAutoIndexCreationQueryStringName))
            {
                DisableAutoIndexCreation = GetBoolValue(name, pair.EncodedValue);
                return;
            }

            if (IsMatch(name, DebugQueryStringName))
            {
                Debug = pair.DecodeValue().ToString();
                return;
            }

            if (IsMatch(name, IgnoreLimitQueryStringName))
            {
                IgnoreLimit = GetBoolValue(name, pair.EncodedValue);
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
