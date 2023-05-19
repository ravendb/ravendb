using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Config;
using Raven.Server.Documents.Handlers.Processors.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.Documents.Sharding.Queries.Facets;
using Raven.Server.Documents.Sharding.Queries.IndexEntries;
using Raven.Server.Documents.Sharding.Queries.Suggestions;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

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

    protected override async ValueTask<IndexEntriesQueryResult> GetIndexEntriesAsync(TransactionOperationContext queryContext, TransactionOperationContext context, IndexQueryServerSide query, long? existingResultEtag,
        bool ignoreLimit, OperationCancelToken token)
    {
        using (var timings = Timings(query))
        {
            var indexName = AbstractQueryRunner.GetIndexName(query);

            using (RequestHandler.DatabaseContext.QueryRunner.MarkQueryAsRunning(indexName, query, token))
            {
                var queryProcessor = new ShardedIndexEntriesQueryProcessor(queryContext, RequestHandler, query, existingResultEtag, ignoreLimit, token.Token);

                await queryProcessor.InitializeAsync();

                var result = await queryProcessor.ExecuteShardedOperations(timings.Scope);

                result.DurationInMs = timings.Duration;

                return result;
            }
        }
    }

    protected override async ValueTask ExplainAsync(TransactionOperationContext queryContext, IndexQueryServerSide query, OperationCancelToken token)
    {
        var command = new ExplainQueryCommand(DocumentConventions.DefaultForServer, query.ToJson(queryContext));

        var proxyCommand = new ProxyCommand<ExplainQueryCommand.ExplainQueryResult[]>(command, HttpContext.Response);

        await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(queryContext, proxyCommand, shardNumber: 0, token.Token);
    }

    protected override AbstractDatabaseNotificationCenter NotificationCenter => RequestHandler.DatabaseContext.NotificationCenter;

    protected override RavenConfiguration Configuration => RequestHandler.DatabaseContext.Configuration;

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
            var indexName = AbstractQueryRunner.GetIndexName(query);

            using (RequestHandler.DatabaseContext.QueryRunner.MarkQueryAsRunning(indexName, query, token))
            {
                var queryProcessor = new ShardedQueryProcessor(queryContext, RequestHandler, query, existingResultEtag, metadataOnly, token.Token);

                await queryProcessor.InitializeAsync();

                var result = await queryProcessor.ExecuteShardedOperations(timings.Scope);

                result.DurationInMs = timings.Duration;

                return result;
            }
        }
    }

    protected override void AssertIndexQuery(IndexQueryServerSide indexQuery)
    {
        if (indexQuery.Diagnostics != null)
            throw new NotSupportedInShardingException("Query diagnostics for a sharded database are currently not supported.");

        if (indexQuery.Metadata.HasMoreLikeThis)
            throw new NotSupportedInShardingException("MoreLikeThis queries are currently not supported in a sharded database ");
        
        if (indexQuery.Metadata.HasHighlightings)
            throw new NotSupportedInShardingException("Highlighting queries are currently not supported in a sharded database ");
        
        if (indexQuery.Metadata.HasIntersect && indexQuery.Metadata.OrderBy?.Length > 0)
            throw new NotSupportedInShardingException("Ordered intersect queries are currently not supported in a sharded database ");
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
