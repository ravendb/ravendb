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
    protected TQueryContext QueryContext;
    protected TOperationContext OperationContext;
    protected RequestTimeTracker Tracker;
    public IndexQueryServerSide IndexQuery;
    protected OperationCancelToken Token;

    protected AbstractQueriesHandlerProcessorForGet([NotNull] TRequestHandler requestHandler, QueryMetadataCache queryMetadataCache, HttpMethod method) : base(requestHandler, queryMetadataCache)
    {
        QueryMethod = method;
        _parameters = QueryStringParameters.Create(HttpContext.Request);
    }

    internal abstract void AllocateContextForQueryOperation(out TQueryContext queryContext, out TOperationContext context);

    private ValueTask HandleDebugAsync(IndexQueryServerSide query, QueryStringParameters parameters, long? existingResultEtag)
    {
        var debug = parameters.Debug;
        if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
        {
            var ignoreLimit = parameters.IgnoreLimit;
            return IndexEntriesAsync(query, existingResultEtag, ignoreLimit);
        }

        if (string.Equals(debug, "explain", StringComparison.OrdinalIgnoreCase))
        {
            return ExplainAsync(query);
        }

        if (string.Equals(debug, "serverSideQuery", StringComparison.OrdinalIgnoreCase))
        {
            return ServerSideQueryAsync(query);
        }

        return ValueTask.FromException(new NotSupportedException($"Not supported query debug operation: '{debug}'"));
    }

    protected abstract Task<IndexEntriesQueryResult> GetIndexEntriesAsync(IndexQueryServerSide query, long? existingResultEtag, bool ignoreLimit);

    private async ValueTask IndexEntriesAsync(IndexQueryServerSide query, long? existingResultEtag, bool ignoreLimit)
    {
        var result = await GetIndexEntriesAsync(query, existingResultEtag, ignoreLimit);

        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        await using (var writer = new AsyncBlittableJsonTextWriter(OperationContext, RequestHandler.ResponseBodyStream(), Token.Token))
        {
            var writeIndexEntriesQueryResultsTask = writer.WriteIndexEntriesQueryResultAsync(OperationContext, result, Token.Token);
            if (writeIndexEntriesQueryResultsTask.IsCompletedSuccessfully == false)
                await writeIndexEntriesQueryResultsTask;
        }
    }

    protected abstract ValueTask ExplainAsync(IndexQueryServerSide query);

    protected abstract Task<FacetedQueryResult> GetFacetedQueryResultAsync(IndexQueryServerSide query, long? existingResultEtag);

    protected abstract Task<SuggestionQueryResult> GetSuggestionQueryResultAsync(IndexQueryServerSide query, long? existingResultEtag);

    protected abstract Task<TQueryResultsContainer> GetQueryResultsAsync(IndexQueryServerSide query, long? existingResultEtag, bool metadataOnly);

    protected override HttpMethod QueryMethod { get; }
    
    internal async ValueTask ExecuteWithExceptionHandling(ValueTask task)
    {
        try
        {
            await task;
        }
        catch (Exception e)
        {
            ProcessQueryException(e);
            throw;
        }
    }

    internal void ProcessQueryException(Exception e)
    {
        if (Tracker.Query == null)
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

            Tracker.Query = errorMessage;

            if (TrafficWatchManager.HasRegisteredClients)
                RequestHandler.AddStringToHttpContext(errorMessage, TrafficWatchChangeType.Queries);
        }
    }

    public void Initialize()
    {
        AllocateContextForQueryOperation(out QueryContext, out OperationContext);
        Tracker = CreateRequestTimeTracker();
        Token = CreateHttpRequestBoundTimeLimitedOperationTokenForQuery();
    }

    public ValueTask<IndexQueryServerSide> ReadIndexQueryForPost()
    {
        return ReadIndexQueryForPost(OperationContext, Tracker, AddSpatialProperties);
    }

    public void LoadIndexQueryForGet()
    {
        try
        {
            IndexQuery = ReadIndexQueryForGet(OperationContext, Tracker, AddSpatialProperties);
        }
        catch (Exception e)
        {
            ProcessQueryException(e);
            throw;
        }
    }

    private async ValueTask HandleIndexQueryAsync(IndexQueryServerSide indexQuery, long? existingResultEtag, QueryStringParameters parameters)
    {
        TQueryResultsContainer result = null;
        try
        {
            result = await GetQueryResultsAsync(indexQuery, existingResultEtag, parameters.MetadataOnly);
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
            var writer = new AsyncBlittableJsonTextWriter(OperationContext, RequestHandler.ResponseBodyStream(), Token.Token);
            try
            {
                result.Timings = indexQuery.Timings?.ToTimings();
                var writeResultsTask = writer.WriteDocumentQueryResultAsync(OperationContext, result, parameters.MetadataOnly,
                    WriteAdditionalData(indexQuery, parameters.IncludeServerSideQuery), Token.Token);
                (numberOfResults, totalDocumentsSizeInBytes) = writeResultsTask.IsCompletedSuccessfully 
                    ? writeResultsTask.Result 
                    : await writeResultsTask;
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

    public override ValueTask ExecuteAsync()
    {
        IndexQuery.Diagnostics = _parameters.Diagnostics ? new List<string>() : null;
        IndexQuery.AddTimeSeriesNames = _parameters.AddTimeSeriesNames;
        IndexQuery.DisableAutoIndexCreation = _parameters.DisableAutoIndexCreation;

        if (RequestHandler.HttpContext.Request.IsFromOrchestrator())
            IndexQuery.ReturnOptions = IndexQueryServerSide.QueryResultReturnOptions.CreateForSharding(IndexQuery);

        AssertIndexQuery(IndexQuery);

        var existingResultEtag = RequestHandler.GetLongFromHeaders(Constants.Headers.IfNoneMatch);

        EnsureQueryContextInitialized();
        return ProcessQueryAsync();


        ValueTask ProcessQueryAsync()
        {
            if (string.IsNullOrWhiteSpace(_parameters.Debug) == false)
            {
                return HandleDebugAsync(IndexQuery, _parameters, existingResultEtag);
            }

            if (TrafficWatchManager.HasRegisteredClients)
                RequestHandler.TrafficWatchQuery(IndexQuery);

            if (IndexQuery.Metadata.HasFacet)
            {
                return HandleFacetedQueryAsync(IndexQuery, existingResultEtag);
            }

            if (IndexQuery.Metadata.HasSuggest)
            {
                return HandleSuggestQueryAsync(IndexQuery, existingResultEtag);
            }

            return HandleIndexQueryAsync(IndexQuery, existingResultEtag, _parameters);
        }
    }


    protected virtual void AssertIndexQuery(IndexQueryServerSide indexQuery)
    {
    }

    protected virtual void EnsureQueryContextInitialized()
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

    private async ValueTask ServerSideQueryAsync(IndexQueryServerSide indexQuery)
    {
        await using (var writer = new AsyncBlittableJsonTextWriter(OperationContext, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(indexQuery.ServerSideQuery));
            writer.WriteString(indexQuery.ServerSideQuery);

            writer.WriteEndObject();
        }
    }

    private async ValueTask HandleSuggestQueryAsync(IndexQueryServerSide query, long? existingResultEtag)
    {
        var result = await GetSuggestionQueryResultAsync(query, existingResultEtag);

        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        long numberOfResults;
        long totalDocumentsSizeInBytes;
        await using (var writer = new AsyncBlittableJsonTextWriter(OperationContext, RequestHandler.ResponseBodyStream(), Token.Token))
        {
            var writeSuggestionQueryResultTask = writer.WriteSuggestionQueryResultAsync(OperationContext, result, Token.Token);
            (numberOfResults, totalDocumentsSizeInBytes) = writeSuggestionQueryResultTask.IsCompletedSuccessfully
                ? writeSuggestionQueryResultTask.Result
                : await writeSuggestionQueryResultTask;
        }

        if (RequestHandler.ShouldAddPagingPerformanceHint(numberOfResults))
            RequestHandler.AddPagingPerformanceHint(PagingOperationType.Queries, $"SuggestQuery ({result.IndexName})", query.Query, numberOfResults, query.PageSize, result.DurationInMs, totalDocumentsSizeInBytes);
    }

    private async ValueTask HandleFacetedQueryAsync(IndexQueryServerSide query, long? existingResultEtag)
    {
        var result = await GetFacetedQueryResultAsync(query, existingResultEtag);

        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        long numberOfResults;
        await using (var writer = new AsyncBlittableJsonTextWriter(OperationContext, RequestHandler.ResponseBodyStream(), Token.Token))
        {
            result.Timings = query.Timings?.ToTimings();
            numberOfResults = await writer.WriteFacetedQueryResultAsync(OperationContext, result, Token.Token);
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
