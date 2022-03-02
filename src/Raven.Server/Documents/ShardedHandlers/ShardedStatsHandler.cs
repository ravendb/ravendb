using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Routing;
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
                    Json.BlittableJsonTextWriterExtensions.WriteDatabaseStatistics(writer, null, statistics);
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
                    State = index.State ?? IndexState.Error,
                    Name = index.Name,
                    LockMode = index.LockMode ?? IndexLockMode.LockedError,
                    Priority = index.Priority ?? new IndexPriority(),
                    Type = index.Type,
                    SourceType = index.SourceType
                };

                i++;
            }

            return indexInformation;
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
                for (int i = 0; i < span.Length; i++)
                {
                    var result = span[i];
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

                combined.SizeOnDisk = new Size(totalSizeOnDisk);
                combined.TempBuffersSizeOnDisk = new Size(totalTempBuffersSizeOnDisk);

                return combined;
            }

            public RavenCommand<DatabaseStatistics> CreateCommandForShard(int shard) => new GetStatisticsOperation.GetStatisticsCommand(debugTag: null, nodeTag: null);
        }
    }

}
