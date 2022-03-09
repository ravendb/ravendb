using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Processors
{
    internal class ShardedStatsHandlerProcessorForGetDatabaseStatistics : AbstractStatsHandlerProcessorForGetDatabaseStatistics<ShardedRequestHandler, TransactionOperationContext>
    {
        public ShardedStatsHandlerProcessorForGetDatabaseStatistics([NotNull] ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask<DatabaseStatistics> GetDatabaseStatisticsAsync()
        {
            var op = new ShardedStatsOperation();

            var stats = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
            stats.Indexes = GetDatabaseIndexesFromRecord(RequestHandler.ShardedContext.DatabaseRecord);
            stats.CountOfIndexes = stats.Indexes.Length;

            return stats;
        }

        internal static IndexInformation[] GetDatabaseIndexesFromRecord(DatabaseRecord record)
        {
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

        internal readonly struct ShardedStatsOperation : IShardedOperation<DatabaseStatistics>
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

            internal static void FillDatabaseStatistics(DatabaseStatistics combined, DatabaseStatistics result, ref long totalSizeOnDisk, ref long totalTempBuffersSizeOnDisk)
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
        }
    }
}
