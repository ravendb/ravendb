using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedStatsHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/stats", "GET")]
        public async Task Stats()
        {
            var op = new ShardedStatsOperation();

            var statistics = await ShardExecutor.ExecuteParallelForAllAsync(op);
            statistics.Indexes = GetDatabaseIndexesFromRecord();
            statistics.CountOfIndexes = statistics.Indexes.Length;

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    Json.BlittableJsonTextWriterExtensions.WriteDatabaseStatistics(writer, context, statistics);
                }
            }
        }

        [RavenShardedAction("/databases/*/stats/detailed", "GET")]
        public async Task DetailedStats()
        {
            var op = new ShardedDetailedStatsOperation();

            var detailedStatistics = await ShardExecutor.ExecuteParallelForAllAsync(op);
            detailedStatistics.Indexes = GetDatabaseIndexesFromRecord();
            detailedStatistics.CountOfIndexes = detailedStatistics.Indexes.Length;

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext serverContext))
            using (serverContext.OpenReadTransaction())
            {
                detailedStatistics.CountOfIdentities = ServerStore.Cluster.GetNumberOfIdentities(serverContext, ShardedContext.DatabaseName);
                detailedStatistics.CountOfCompareExchange = ServerStore.Cluster.GetNumberOfCompareExchange(serverContext, ShardedContext.DatabaseName);
                detailedStatistics.CountOfCompareExchangeTombstones = ServerStore.Cluster.GetNumberOfCompareExchangeTombstones(serverContext, ShardedContext.DatabaseName);
            }

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    Json.BlittableJsonTextWriterExtensions.WriteDetailedDatabaseStatistics(writer, context, detailedStatistics);
                }
            }
        }

        private IndexInformation[] GetDatabaseIndexesFromRecord()
        {
            var record = ShardedContext.DatabaseRecord;
            var indexes = record.Indexes;
            var indexInformation = new IndexInformation[indexes.Count];

            int i = 0;
            foreach (var key in indexes.Keys)
            {
                var index = indexes[key];

                indexInformation[i] = new IndexInformation
                {
                    Name = index.Name,
                    // IndexDefinition includes nullable fields, then in case of null we set to default values
                    State = index.State ?? IndexState.Normal, 
                    LockMode = index.LockMode ?? IndexLockMode.Unlock,
                    Priority = index.Priority ?? IndexPriority.Normal,
                    Type = index.Type,
                    SourceType = index.SourceType,
                    IsStale = false // for sharding we can't determine 
                };

                i++;
            }

            return indexInformation;
        }

        private static void FillDatabaseStatistics(DatabaseStatistics combined, DatabaseStatistics result, ref long totalSizeOnDisk, ref long totalTempBuffersSizeOnDisk)
        {
            combined.CountOfDocuments += result.CountOfDocuments;
            combined.CountOfAttachments += result.CountOfAttachments;
            combined.CountOfConflicts += result.CountOfConflicts;
            combined.CountOfCounterEntries += result.CountOfCounterEntries;
            combined.CountOfDocumentsConflicts += result.CountOfDocumentsConflicts;
            combined.CountOfRevisionDocuments += result.CountOfRevisionDocuments;
            combined.CountOfTimeSeriesSegments += result.CountOfTimeSeriesSegments;
            combined.CountOfTombstones += result.CountOfTombstones;
            totalSizeOnDisk += result.SizeOnDisk.SizeInBytes;
            totalTempBuffersSizeOnDisk += result.TempBuffersSizeOnDisk.SizeInBytes;
        }

        private readonly struct ShardedStatsOperation : IShardedOperation<DatabaseStatistics>
        {
            public DatabaseStatistics Combine(Memory<DatabaseStatistics> results)
            {
                var span = results.Span;

                var combined = new DatabaseStatistics
                {
                    DatabaseChangeVector = null,
                    DatabaseId = null,
                    Indexes = Array.Empty<IndexInformation>()
                };

                long totalSizeOnDisk = 0;
                long totalTempBuffersSizeOnDisk = 0;
                foreach (var result in span)
                {
                    FillDatabaseStatistics(combined, result, ref totalSizeOnDisk, ref totalTempBuffersSizeOnDisk);
                }

                combined.SizeOnDisk = new Size(totalSizeOnDisk);
                combined.TempBuffersSizeOnDisk = new Size(totalTempBuffersSizeOnDisk);

                return combined;
            }

            public RavenCommand<DatabaseStatistics> CreateCommandForShard(int shard) => new GetStatisticsOperation.GetStatisticsCommand(debugTag: null, nodeTag: null);
        }

        private readonly struct ShardedDetailedStatsOperation : IShardedOperation<DetailedDatabaseStatistics>
        {
            public DetailedDatabaseStatistics Combine(Memory<DetailedDatabaseStatistics> results)
            {
                var span = results.Span;
                if (span.Length == 0)
                    return null;

                var combined = new DetailedDatabaseStatistics
                {
                    DatabaseChangeVector = null,
                    DatabaseId = null,
                    Indexes = Array.Empty<IndexInformation>()
                };

                long totalSizeOnDisk = 0;
                long totalTempBuffersSizeOnDisk = 0;
                foreach (var result in span)
                {
                    FillDatabaseStatistics(combined, result, ref totalSizeOnDisk, ref totalTempBuffersSizeOnDisk);
                    combined.CountOfTimeSeriesDeletedRanges += result.CountOfTimeSeriesDeletedRanges;
                }

                combined.SizeOnDisk = new Size(totalSizeOnDisk);
                combined.TempBuffersSizeOnDisk = new Size(totalTempBuffersSizeOnDisk);

                return combined;
            }

            public RavenCommand<DetailedDatabaseStatistics> CreateCommandForShard(int shard) => new GetDetailedStatisticsOperation.DetailedDatabaseStatisticsCommand(debugTag: null);
        }
    }

}
