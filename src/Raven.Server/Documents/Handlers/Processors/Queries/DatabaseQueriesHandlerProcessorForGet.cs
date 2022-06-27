using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Queries;

internal class DatabaseQueriesHandlerProcessorForGet : AbstractQueriesHandlerProcessorForGet<QueriesHandler, DocumentsOperationContext, QueryOperationContext, Document>
{
    public DatabaseQueriesHandlerProcessorForGet([NotNull] QueriesHandler requestHandler, HttpMethod method) : base(requestHandler, requestHandler.Database.QueryMetadataCache, method)
    {
    }

    protected override IDisposable AllocateContextForQueryOperation(out QueryOperationContext queryContext, out DocumentsOperationContext context)
    {
        queryContext = QueryOperationContext.Allocate(RequestHandler.Database);

        context = queryContext.Documents;

        return queryContext;
    }

    protected override async ValueTask HandleDebug(IndexQueryServerSide query, QueryOperationContext queryContext, string debug, long? existingResultEtag,
        OperationCancelToken token)
    {
        if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
        {
            var ignoreLimit = RequestHandler.GetBoolValueQueryString("ignoreLimit", required: false) ?? false;
            await IndexEntries(queryContext, query, existingResultEtag, token, ignoreLimit);
            return;
        }

        if (string.Equals(debug, "explain", StringComparison.OrdinalIgnoreCase))
        {
            await Explain(queryContext, query);
            return;
        }

        if (string.Equals(debug, "serverSideQuery", StringComparison.OrdinalIgnoreCase))
        {
            await ServerSideQuery(queryContext, query);
            return;
        }

        throw new NotSupportedException($"Not supported query debug operation: '{debug}'");
    }

    protected override async ValueTask HandleFacetedQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
    {
        var result = await RequestHandler.Database.QueryRunner.ExecuteFacetedQuery(query, existingResultEtag, queryContext, token);

        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        long numberOfResults;
        await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, RequestHandler.ResponseBodyStream()))
        {
            numberOfResults = await writer.WriteFacetedQueryResultAsync(queryContext.Documents, result, token.Token);
        }

        QueryMetadataCache.MaybeAddToCache(query.Metadata, result.IndexName);

        if (RequestHandler.ShouldAddPagingPerformanceHint(numberOfResults))
            RequestHandler.AddPagingPerformanceHint(PagingOperationType.Queries, $"FacetedQuery ({result.IndexName})", $"{query.Metadata.QueryText}\n{query.QueryParameters}", numberOfResults, query.PageSize, result.DurationInMs, -1);
    }

    protected override async ValueTask HandleSuggestQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
    {
        var result = await RequestHandler.Database.QueryRunner.ExecuteSuggestionQuery(query, queryContext, existingResultEtag, token);
        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        long numberOfResults;
        long totalDocumentsSizeInBytes;
        await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, RequestHandler.ResponseBodyStream()))
        {
            (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteSuggestionQueryResultAsync(queryContext.Documents, result, token.Token);
        }

        if (RequestHandler.ShouldAddPagingPerformanceHint(numberOfResults))
            RequestHandler.AddPagingPerformanceHint(PagingOperationType.Queries, $"SuggestQuery ({result.IndexName})", query.Query, numberOfResults, query.PageSize, result.DurationInMs, totalDocumentsSizeInBytes);
    }

    protected override async ValueTask<QueryResultServerSide<Document>> GetQueryResults(IndexQueryServerSide query, QueryOperationContext queryContext,
        long? existingResultEtag, OperationCancelToken token)
    {
        return await RequestHandler.Database.QueryRunner.ExecuteQuery(query, queryContext, existingResultEtag, token).ConfigureAwait(false);
    }

    private async Task IndexEntries(QueryOperationContext queryContext, IndexQueryServerSide indexQuery, long? existingResultEtag, OperationCancelToken token, bool ignoreLimit)
    {
        var result = await RequestHandler.Database.QueryRunner.ExecuteIndexEntriesQuery(indexQuery, queryContext, ignoreLimit, existingResultEtag, token);

        if (result.NotModified)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
            return;
        }

        HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

        await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, RequestHandler.ResponseBodyStream()))
        {
            await writer.WriteIndexEntriesQueryResultAsync(queryContext.Documents, result, token.Token);
        }
    }

    private async Task Explain(QueryOperationContext queryContext, IndexQueryServerSide indexQuery)
    {
        var explanations = RequestHandler.Database.QueryRunner.ExplainDynamicIndexSelection(indexQuery, out string indexName);

        await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("IndexName");
            writer.WriteString(indexName);
            writer.WriteComma();
            writer.WriteArray(queryContext.Documents, "Results", explanations, (w, c, explanation) => w.WriteExplanation(queryContext.Documents, explanation));

            writer.WriteEndObject();
        }
    }

    private async Task ServerSideQuery(QueryOperationContext queryContext, IndexQueryServerSide indexQuery)
    {
        await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(indexQuery.ServerSideQuery));
            writer.WriteString(indexQuery.ServerSideQuery);

            writer.WriteEndObject();
        }
    }
}
