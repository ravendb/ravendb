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
    internal sealed class ShardedCollectionsHandlerProcessorForGetCollectionStats : AbstractCollectionsHandlerProcessorForGetCollectionStats<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedCollectionsHandlerProcessorForGetCollectionStats([NotNull] ShardedDatabaseRequestHandler requestHandler, bool detailed) : base(requestHandler, detailed)
        {
        }

        protected override async ValueTask<DynamicJsonValue> GetStatsAsync(TransactionOperationContext context, bool detailed)
        {
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                if (detailed)
                {
                    var stats = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new ShardedDetailedCollectionStatisticsOperation(HttpContext), token.Token);
                    return stats.ToJson();
                }
                else
                {
                    var stats = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new ShardedCollectionStatisticsOperation(HttpContext), token.Token);
                    return stats.ToJson();
                }
            }
        }
    }

    public readonly struct ShardedCollectionStatisticsOperation : IShardedOperation<CollectionStatistics>
    {
        private readonly HttpContext _httpContext;

        public ShardedCollectionStatisticsOperation(HttpContext httpContext)
        {
            _httpContext = httpContext;
        }

        public HttpRequest HttpRequest => _httpContext?.Request;

        public CollectionStatistics Combine(Dictionary<int, ShardExecutionResult<CollectionStatistics>> results)
        {
            var stats = new CollectionStatistics();
            
            foreach (var shardResult in results.Values)
            {
                var shardStats = shardResult.Result;
                stats.CountOfDocuments += shardStats.CountOfDocuments;
                stats.CountOfConflicts += shardStats.CountOfConflicts;
                foreach (var collectionInfo in shardStats.Collections)
                {
                    stats.Collections[collectionInfo.Key] = stats.Collections.ContainsKey(collectionInfo.Key)
                        ? stats.Collections[collectionInfo.Key] + collectionInfo.Value
                        : collectionInfo.Value;
                }
            }

            return stats;
        }

        public RavenCommand<CollectionStatistics> CreateCommandForShard(int shardNumber) => new GetCollectionStatisticsOperation.GetCollectionStatisticsCommand();
    }

    public readonly struct ShardedDetailedCollectionStatisticsOperation : IShardedOperation<DetailedCollectionStatistics>
    {
        private readonly HttpContext _httpContext;

        public ShardedDetailedCollectionStatisticsOperation(HttpContext httpContext)
        {
            _httpContext = httpContext;
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public DetailedCollectionStatistics Combine(Dictionary<int, ShardExecutionResult<DetailedCollectionStatistics>> results)
        {
            var stats = new DetailedCollectionStatistics();
            foreach (var result in results.Values)
            {
                stats.CountOfDocuments += result.Result.CountOfDocuments;
                stats.CountOfConflicts += result.Result.CountOfConflicts;
                foreach (var collectionInfo in result.Result.Collections)
                {
                    if (stats.Collections.ContainsKey(collectionInfo.Key))
                    {
                        stats.Collections[collectionInfo.Key].CountOfDocuments += collectionInfo.Value.CountOfDocuments;
                        stats.Collections[collectionInfo.Key].DocumentsSize.SizeInBytes += collectionInfo.Value.DocumentsSize.SizeInBytes;
                        stats.Collections[collectionInfo.Key].RevisionsSize.SizeInBytes += collectionInfo.Value.RevisionsSize.SizeInBytes;
                        stats.Collections[collectionInfo.Key].TombstonesSize.SizeInBytes += collectionInfo.Value.TombstonesSize.SizeInBytes;
                    }
                    else
                    {
                        stats.Collections[collectionInfo.Key] = collectionInfo.Value;
                    }
                }
            }

            return stats;
        }

        public RavenCommand<DetailedCollectionStatistics> CreateCommandForShard(int shardNumber) =>
            new GetDetailedCollectionStatisticsOperation.GetDetailedCollectionStatisticsCommand();
    }
}
