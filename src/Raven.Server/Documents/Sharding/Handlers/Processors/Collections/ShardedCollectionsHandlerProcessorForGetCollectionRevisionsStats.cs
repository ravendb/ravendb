using System;
using System.Collections.Generic;
using System.Threading;
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

        protected override async ValueTask<DynamicJsonValue> GetStatsAsync(TransactionOperationContext context, CancellationToken token)
        {
            var stats = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new ShardedCollectionRevisionsStatisticsOperation(HttpContext), token);
            return stats.ToJson();
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
            var combined = new CollectionRevisionsStatistics
            {
                Collections = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
            };

            foreach (var shardResult in results.Values)
            {
                var shardStats = shardResult.Result;
                combined.CountOfRevisions += shardStats.CountOfRevisions;
                foreach (var collectionInfo in shardStats.Collections)
                {
                    combined.Collections.TryAdd(collectionInfo.Key, 0);
                    combined.Collections[collectionInfo.Key] += collectionInfo.Value;
                }
            }

            return combined;
        }

        public RavenCommand<CollectionRevisionsStatistics> CreateCommandForShard(int shardNumber) => new GetCollectionRevisionsStatisticsOperation.GetCollectionRevisionsStatisticsCommand();
    }
}
