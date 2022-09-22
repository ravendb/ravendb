using System;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Processors.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Queries;

internal class ShardedQueriesHandlerProcessorForGet : AbstractQueriesHandlerProcessorForGet<ShardedQueriesHandler, TransactionOperationContext, TransactionOperationContext, BlittableJsonReaderObject>
{
    public ShardedQueriesHandlerProcessorForGet([NotNull] ShardedQueriesHandler requestHandler, HttpMethod method) : base(requestHandler, requestHandler.DatabaseContext.QueryMetadataCache, method)
    {
    }

    protected override IDisposable AllocateContextForQueryOperation(out TransactionOperationContext queryContext, out TransactionOperationContext context)
    {
        var returnContext = ContextPool.AllocateOperationContext(out queryContext);

        context = queryContext;

        return returnContext;
    }

    protected override AbstractDatabaseNotificationCenter NotificationCenter => RequestHandler.DatabaseContext.NotificationCenter;

    protected override RavenConfiguration Configuration => RequestHandler.DatabaseContext.Configuration;

    protected override ValueTask HandleDebug(IndexQueryServerSide query, TransactionOperationContext queryContext, string debug, long? existingResultEtag, OperationCancelToken token)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19071 Implement debug");

        throw new NotSupportedInShardingException("Query debug is not supported");
    }

    protected override ValueTask HandleFacetedQuery(IndexQueryServerSide query, TransactionOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "Implement facets - RavenDB-18765");

        throw new NotSupportedInShardingException("Facets are not supported");
    }

    protected override ValueTask HandleSuggestQuery(IndexQueryServerSide query, TransactionOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "Implement suggest - RavenDB-18857");

        throw new NotSupportedInShardingException("Suggestions are not supported");
    }

    protected override async ValueTask<QueryResultServerSide<BlittableJsonReaderObject>> GetQueryResults(IndexQueryServerSide query,
        TransactionOperationContext queryContext, long? existingResultEtag, bool metadataOnly, OperationCancelToken token)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal,
            @"RavenDB-19071 what do we do with: var diagnostics = GetBoolValueQueryString(""diagnostics"", required: false) ?? false");

        var indexName = AbstractQueryRunner.GetIndexName(query);

        using (RequestHandler.DatabaseContext.QueryRunner.MarkQueryAsRunning(indexName, query, token))
        {
            var queryProcessor = new ShardedQueryProcessor(queryContext, RequestHandler, query, existingResultEtag, metadataOnly, indexEntriesOnly: false,
                token: token.Token);

            queryProcessor.Initialize();

            return await queryProcessor.ExecuteShardedOperations();
        }
    }
}
