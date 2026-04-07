using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
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

internal sealed class DatabaseQueriesHandlerProcessorForGet : AbstractQueriesHandlerProcessorForGet<QueriesHandler, DocumentsOperationContext, QueryOperationContext, Document, DocumentQueryResult>
{
    private QueryOperationContext _queryContext;
    
    public DatabaseQueriesHandlerProcessorForGet([NotNull] QueriesHandler requestHandler, HttpMethod method) : base(requestHandler, requestHandler.Database.QueryMetadataCache, method)
    {
        RegisterForDisposal(this);
        Initialize();
    }

    internal override void AllocateContextForQueryOperation(out QueryOperationContext queryContext, out DocumentsOperationContext context)
    {
        if (_queryContext != null)
        {
            queryContext = _queryContext;
            context = _queryContext.Documents;
            return;
        }
        
        _queryContext = QueryOperationContext.Allocate(RequestHandler.Database);
        queryContext = _queryContext;
        context = _queryContext.Documents;

        RegisterForDisposal(_queryContext);
    }

    protected override AbstractDatabaseNotificationCenter NotificationCenter => RequestHandler.Database.NotificationCenter;

    protected override RavenConfiguration Configuration => RequestHandler.Database.Configuration;

    protected override Task<FacetedQueryResult> GetFacetedQueryResultAsync(IndexQueryServerSide query, long? existingResultEtag)
    {
        return RequestHandler.Database.QueryRunner.ExecuteFacetedQuery(query, existingResultEtag, QueryContext, Token);
    }

    protected override Task<SuggestionQueryResult> GetSuggestionQueryResultAsync(IndexQueryServerSide query, long? existingResultEtag)
    {
        return RequestHandler.Database.QueryRunner.ExecuteSuggestionQuery(query, QueryContext, existingResultEtag, Token);
    }

    protected override Task<DocumentQueryResult> GetQueryResultsAsync(IndexQueryServerSide query, long? existingResultEtag, bool metadataOnly)
    {
        return RequestHandler.Database.QueryRunner.ExecuteQuery(query, QueryContext, existingResultEtag, Token);
    }

    protected override void EnsureQueryContextInitialized()
    {
        Debug.Assert(QueryContext is not null);
        Debug.Assert(IndexQuery is not null);
        QueryContext.WithQuery(IndexQuery.Metadata);
    }

    protected override Task<IndexEntriesQueryResult> GetIndexEntriesAsync(IndexQueryServerSide query, long? existingResultEtag, bool ignoreLimit)
    {
        return RequestHandler.Database.QueryRunner.ExecuteIndexEntriesQuery(query, _queryContext, ignoreLimit, existingResultEtag, Token);
    }

    protected override async ValueTask ExplainAsync(IndexQueryServerSide query)
    {
        var explanations = RequestHandler.Database.QueryRunner.ExplainDynamicIndexSelection(query, out string indexName);

        await using (var writer = new AsyncBlittableJsonTextWriter(_queryContext.Documents, RequestHandler.ResponseBodyStream(), Token.Token))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("IndexName");
            writer.WriteString(indexName);
            writer.WriteComma();
            writer.WriteArray(_queryContext.Documents, "Results", explanations, (w, c, explanation) => w.WriteExplanation(_queryContext.Documents, explanation));

            writer.WriteEndObject();
        }
    }
}
