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

internal sealed class ShardedQueriesHandlerProcessorForGet : AbstractQueriesHandlerProcessorForGet<ShardedQueriesHandler, TransactionOperationContext, TransactionOperationContext, BlittableJsonReaderObject, QueryResultServerSide<BlittableJsonReaderObject>>
{
    private TransactionOperationContext _queryContext;
    
    public ShardedQueriesHandlerProcessorForGet([NotNull] ShardedQueriesHandler requestHandler, HttpMethod method) : base(requestHandler, requestHandler.DatabaseContext.QueryMetadataCache, method)
    {
        RegisterForDisposal(this);
        Initialize();
    }

    internal override void AllocateContextForQueryOperation(out TransactionOperationContext queryContext, out TransactionOperationContext context)
    {
        if (_queryContext != null)
            throw new InvalidOperationException("Context is already allocated.");
        
        var returnContext = ContextPool.AllocateOperationContext(out _queryContext);
        context = _queryContext;
        queryContext = _queryContext;
        RegisterForDisposal(returnContext);
    }

    protected override async Task<IndexEntriesQueryResult> GetIndexEntriesAsync(IndexQueryServerSide query, long? existingResultEtag, bool ignoreLimit)
    {
        using (var timings = Timings(query))
        {
            var indexName = AbstractQueryRunner.GetIndexName(query);

            using (RequestHandler.DatabaseContext.QueryRunner.MarkQueryAsRunning(indexName, query, Token))
            {
                var queryProcessor = new ShardedIndexEntriesQueryProcessor(QueryContext, RequestHandler, query, existingResultEtag, ignoreLimit, Token.Token);

                await queryProcessor.InitializeAsync();

                var result = await queryProcessor.ExecuteShardedOperations(timings.Scope);

                result.DurationInMs = timings.Duration;

                return result;
            }
        }
    }

    protected override async ValueTask ExplainAsync(IndexQueryServerSide query)
    {
        var command = new ExplainQueryCommand(DocumentConventions.DefaultForServer, query.ToJson(QueryContext));

        var proxyCommand = new ProxyCommand<ExplainQueryCommand.ExplainQueryResult[]>(command, HttpContext);

        await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(QueryContext, proxyCommand, shardNumber: 0, Token.Token);
    }

    protected override AbstractDatabaseNotificationCenter NotificationCenter => RequestHandler.DatabaseContext.NotificationCenter;

    protected override RavenConfiguration Configuration => RequestHandler.DatabaseContext.Configuration;

    protected override async Task<FacetedQueryResult> GetFacetedQueryResultAsync(IndexQueryServerSide query, long? existingResultEtag)
    {
        using (var timings = Timings(query))
        {
            var indexName = AbstractQueryRunner.GetIndexName(query);

            using (RequestHandler.DatabaseContext.QueryRunner.MarkQueryAsRunning(indexName, query, Token))
            {
                var queryProcessor = new ShardedFacetedQueryProcessor(QueryContext, RequestHandler, query, existingResultEtag, Token.Token);

                await queryProcessor.InitializeAsync();

                var result = await queryProcessor.ExecuteShardedOperations(timings.Scope);

                result.DurationInMs = timings.Duration;

                return result;
            }
        }
    }

    protected override async Task<SuggestionQueryResult> GetSuggestionQueryResultAsync(IndexQueryServerSide query, long? existingResultEtag)
    {
        using (var timings = Timings(query))
        {
            var indexName = AbstractQueryRunner.GetIndexName(query);

            using (RequestHandler.DatabaseContext.QueryRunner.MarkQueryAsRunning(indexName, query, Token))
            {
                var queryProcessor = new ShardedSuggestionQueryProcessor(QueryContext, RequestHandler, query, existingResultEtag, Token.Token);

                await queryProcessor.InitializeAsync();

                var result = await queryProcessor.ExecuteShardedOperations(timings.Scope);

                result.DurationInMs = timings.Duration;

                return result;
            }
        }
    }

    protected override async Task<QueryResultServerSide<BlittableJsonReaderObject>> GetQueryResultsAsync(IndexQueryServerSide query, long? existingResultEtag, bool metadataOnly)
    {
        using (var timings = Timings(query))
        {
            var indexName = AbstractQueryRunner.GetIndexName(query);

            using (RequestHandler.DatabaseContext.QueryRunner.MarkQueryAsRunning(indexName, query, Token))
            {
                var queryProcessor = new ShardedQueryProcessor(QueryContext, RequestHandler, query, existingResultEtag, metadataOnly, Token.Token);

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
        
        if (indexQuery.Metadata.HasIntersect)
            throw new NotSupportedInShardingException("Intersect queries are currently not supported in a sharded database ");
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
