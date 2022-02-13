using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers
{
    internal class ShardedCollectionHandler : ShardedRequestHandler
    {

        [RavenShardedAction("/databases/*/collections/stats", "GET")]
        public async Task GetCollectionStats()
        {
            var stats = new CollectionStatistics();
            var res = new List<RavenCommand<CollectionStatistics>>();
            var maintenanceOperations = new List<IMaintenanceOperation<CollectionStatistics>>();

            for (int i = 0; i < ShardedContext.ShardCount; i++)
            {
                maintenanceOperations.Add(new GetCollectionStatisticsOperation());
            }

            await GetShardsResults(res, maintenanceOperations);

            for (int i = 0; i < ShardedContext.ShardCount; i++)
            {
                stats.CountOfDocuments += res[i].Result.CountOfDocuments;
                stats.CountOfConflicts += res[i].Result.CountOfConflicts;
                foreach (var collectionInfo in res[i].Result.Collections)
                {
                    stats.Collections[collectionInfo.Key] = stats.Collections.ContainsKey(collectionInfo.Key) ?
                        stats.Collections[collectionInfo.Key] + collectionInfo.Value :
                        collectionInfo.Value;
                }
            }
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    context.Write(writer, stats.ToJson());
            }
        }

        [RavenShardedAction("/databases/*/collections/stats/detailed", "GET")]
        public async Task GetDetailedCollectionStats()
        {
            var stats = new DetailedCollectionStatistics();
            var res = new List<RavenCommand<DetailedCollectionStatistics>>();
            var maintenanceOperations = new List<IMaintenanceOperation<DetailedCollectionStatistics>>();
            for (int i = 0; i < ShardedContext.ShardCount; i++)
            {
                maintenanceOperations.Add(new GetDetailedCollectionStatisticsOperation());
            }

            await GetShardsResults(res, maintenanceOperations);

            for (int i = 0; i < ShardedContext.ShardCount; i++)
            {
                stats.CountOfDocuments += res[i].Result.CountOfDocuments;
                stats.CountOfConflicts += res[i].Result.CountOfConflicts;
                foreach (var collectionInfo in res[i].Result.Collections)
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
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    context.Write(writer, stats.ToJson());
            }
        }
    }
}
