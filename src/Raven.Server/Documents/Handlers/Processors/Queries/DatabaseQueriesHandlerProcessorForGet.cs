using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
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

    protected override AbstractDatabaseNotificationCenter NotificationCenter => RequestHandler.Database.NotificationCenter;

    protected override RavenConfiguration Configuration => RequestHandler.Database.Configuration;

    protected override async ValueTask<FacetedQueryResult> GetFacetedQueryResultAsync(IndexQueryServerSide query, QueryOperationContext queryContext,
        long? existingResultEtag, OperationCancelToken token)
    {
        return await RequestHandler.Database.QueryRunner.ExecuteFacetedQuery(query, existingResultEtag, queryContext, token);
    }

    protected override async ValueTask<SuggestionQueryResult> GetSuggestionQueryResultAsync(IndexQueryServerSide query, QueryOperationContext queryContext,
        long? existingResultEtag, OperationCancelToken token)
    {
        return await RequestHandler.Database.QueryRunner.ExecuteSuggestionQuery(query, queryContext, existingResultEtag, token);
    }

    protected override async ValueTask<QueryResultServerSide<Document>> GetQueryResultsAsync(IndexQueryServerSide query, QueryOperationContext queryContext,
        long? existingResultEtag, bool metadataOnly, OperationCancelToken token)
    {
        return await RequestHandler.Database.QueryRunner.ExecuteQuery(query, queryContext, existingResultEtag, token).ConfigureAwait(false);
    }

    protected override void EnsureQueryContextInitialized(QueryOperationContext queryContext, IndexQueryServerSide indexQuery)
    {
        queryContext.WithQuery(indexQuery.Metadata);
    }

    protected override async ValueTask IndexEntriesAsync(QueryOperationContext queryContext, IndexQueryServerSide indexQuery, long? existingResultEtag, OperationCancelToken token, bool ignoreLimit)
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

    protected override async ValueTask ExplainAsync(QueryOperationContext queryContext, IndexQueryServerSide indexQuery)
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
}
