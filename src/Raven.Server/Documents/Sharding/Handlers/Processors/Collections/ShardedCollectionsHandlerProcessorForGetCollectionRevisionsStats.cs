using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Collections;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Collections
{
    internal class ShardedCollectionsHandlerProcessorForGetCollectionRevisionsStats : AbstractCollectionsHandlerProcessorForGetCollectionRevisionsStats<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedCollectionsHandlerProcessorForGetCollectionRevisionsStats([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<DynamicJsonValue> GetStatsAsync(TransactionOperationContext context)
        {
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                var stats = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new ShardedCollectionRevisionsStatisticsOperation(HttpContext), token.Token);
                return stats.ToJson();
            }
        }
    }

    public readonly struct ShardedCollectionRevisionsStatisticsOperation : IShardedOperation<CollectionRevisionsStatistics>
    {
        private readonly HttpContext _httpContext;

        public ShardedCollectionRevisionsStatisticsOperation(HttpContext httpContext)
        {
            _httpContext = httpContext;
        }

        public HttpRequest HttpRequest => _httpContext?.Request;

        public CollectionRevisionsStatistics Combine(Dictionary<int, ShardExecutionResult<CollectionRevisionsStatistics>> results)
        {
            var stats = new CollectionRevisionsStatistics() { Collections = new Dictionary<string, long>() };

            foreach (var shardResult in results.Values)
            {
                var shardStats = shardResult.Result;
                stats.CountOfRevisions += shardStats.CountOfRevisions;
                foreach (var collectionInfo in shardStats.Collections)
                {
                    stats.Collections[collectionInfo.Key] = stats.Collections.ContainsKey(collectionInfo.Key)
                        ? stats.Collections[collectionInfo.Key] + collectionInfo.Value
                        : collectionInfo.Value;
                }
            }

            return stats;
        }

        public RavenCommand<CollectionRevisionsStatistics> CreateCommandForShard(int shardNumber) => new GetCollectionRevisionsStatisticsOperation.GetCollectionRevisionsStatisticsCommand();
    }
}
