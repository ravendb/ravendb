using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Processors.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.Documents.Sharding.Queries.Facets;
using Raven.Server.Documents.Sharding.Queries.Suggestions;
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

    protected override ValueTask HandleDebugAsync(IndexQueryServerSide query, TransactionOperationContext queryContext, string debug, long? existingResultEtag, OperationCancelToken token)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19071 Implement debug");

        throw new NotSupportedInShardingException("Query debug is not supported");
    }

    protected override async ValueTask<FacetedQueryResult> GetFacetedQueryResultAsync(IndexQueryServerSide query, TransactionOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
    {
        using (var timings = Timings(query))
        {
            var indexName = AbstractQueryRunner.GetIndexName(query);

            using (RequestHandler.DatabaseContext.QueryRunner.MarkQueryAsRunning(indexName, query, token))
            {
                var queryProcessor = new ShardedFacetedQueryProcessor(queryContext, RequestHandler, query, existingResultEtag, token.Token);

                await queryProcessor.InitializeAsync();

                var result = await queryProcessor.ExecuteShardedOperations(timings.Scope);

                result.DurationInMs = timings.Duration;

                return result;
            }
        }
    }

    protected override async ValueTask<SuggestionQueryResult> GetSuggestionQueryResultAsync(IndexQueryServerSide query, TransactionOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
    {
        using (var timings = Timings(query))
        {
            var indexName = AbstractQueryRunner.GetIndexName(query);

            using (RequestHandler.DatabaseContext.QueryRunner.MarkQueryAsRunning(indexName, query, token))
            {
                var queryProcessor = new ShardedSuggestionQueryProcessor(queryContext, RequestHandler, query, existingResultEtag, token.Token);

                await queryProcessor.InitializeAsync();

                var result = await queryProcessor.ExecuteShardedOperations(timings.Scope);

                result.DurationInMs = timings.Duration;

                return result;
            }
        }
    }

    protected override async ValueTask<QueryResultServerSide<BlittableJsonReaderObject>> GetQueryResultsAsync(IndexQueryServerSide query,
        TransactionOperationContext queryContext, long? existingResultEtag, bool metadataOnly, OperationCancelToken token)
    {
        using (var timings = Timings(query))
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal,
                @"RavenDB-19071 what do we do with: var diagnostics = GetBoolValueQueryString(""diagnostics"", required: false) ?? false");

            var indexName = AbstractQueryRunner.GetIndexName(query);

            using (RequestHandler.DatabaseContext.QueryRunner.MarkQueryAsRunning(indexName, query, token))
            {
                var queryProcessor = new ShardedQueryProcessor(queryContext, RequestHandler, query, existingResultEtag, metadataOnly, indexEntriesOnly: false,
                    token: token.Token);

                await queryProcessor.InitializeAsync();

                var result = await queryProcessor.ExecuteShardedOperations(timings.Scope);

                result.DurationInMs = timings.Duration;

                return result;
            }
        }
    }

    private static TimingsScope Timings(IndexQueryServerSide query) => new(query);

    private readonly struct TimingsScope : IDisposable
    {
        public readonly QueryTimingsScope Scope;

        private readonly Stopwatch _sw;

        public TimingsScope(IndexQueryServerSide query)
        {
            if (query.Timings != null)
            {
                Scope = query.Timings.Start();
                return;
            }

            _sw = Stopwatch.StartNew();
        }

        public long Duration
        {
            get
            {
                if (Scope != null)
                    return (long)Scope.Duration.TotalMilliseconds;

                return (long)_sw.Elapsed.TotalMilliseconds;
            }
        }

        public void Dispose()
        {
            Scope?.Dispose();
        }
    }
}
