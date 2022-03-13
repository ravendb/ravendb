using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors
{
    internal class ShardedIndexHandlerProcessorForGetDatabaseIndexStatistics : AbstractIndexHandlerProcessorForGetDatabaseIndexStatistics<ShardedRequestHandler,
         TransactionOperationContext>
    {
        public ShardedIndexHandlerProcessorForGetDatabaseIndexStatistics([NotNull] ShardedRequestHandler requestHandler) : base(requestHandler,
            requestHandler.ContextPool)
        {
        }

        protected override async ValueTask<IndexStats[]> GetDatabaseIndexStatisticsAsync()
        {
            var op = new ShardedIndexStatsOperation();
            var combined = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);

            return GetIndexStatsFromDatabaseRecord(combined);
        }

        private IndexStats[] GetIndexStatsFromDatabaseRecord(IReadOnlyList<IndexStats> combined)
        {
            var databaseRecord = RequestHandler.ShardedContext.DatabaseRecord;
            var indexes = databaseRecord.Indexes;

            var indexesStats = new IndexStats[indexes.Count];

            int i = 0;
            foreach (var key in indexes.Keys)
            {
                var index = indexes[key];

                var indexStats = combined[i];
                indexStats.Name = index.Name;
                indexStats.State = index.State ?? IndexState.Normal;
                indexStats.LockMode = index.LockMode ?? IndexLockMode.Unlock;
                indexStats.Priority = index.Priority ?? IndexPriority.Normal;
                indexStats.Type = index.Type;
                indexStats.SourceType = index.SourceType;
                indexStats.ReduceOutputCollection = index.OutputReduceToCollection;
                indexStats.PatternReferencesCollectionName = index.PatternReferencesCollectionName;
                indexStats.Status = IndexRunningStatus.Running;
                indexStats.ReduceAttempts = null;
                indexStats.ReduceSuccesses = null;
                indexStats.ReduceErrors = null;
                indexStats.ReduceOutputReferencePattern = null;

                indexesStats[i++] = indexStats;
            }
            return indexesStats;
        }

        internal static void FillIndexStats(IndexStats combined, IndexStats result)
        {
            combined.IsStale &= result.IsStale;
            combined.EntriesCount += result.EntriesCount;
            combined.ErrorsCount += result.ErrorsCount;
            combined.MapAttempts += result.MapAttempts;
            combined.MapSuccesses += result.MapSuccesses;
            combined.MapErrors += result.MapErrors;
            combined.MapReferenceAttempts += result.MapReferenceAttempts;
            combined.MapReferenceSuccesses += result.MapReferenceSuccesses;
            combined.MapReferenceErrors += result.MapReferenceErrors;
            combined.MappedPerSecondRate += result.MappedPerSecondRate;
            combined.ReducedPerSecondRate += result.ReducedPerSecondRate;

            if (combined.CreatedTimestamp.Millisecond > result.CreatedTimestamp.Millisecond)
                combined.CreatedTimestamp = result.CreatedTimestamp;

            if (combined.MaxNumberOfOutputsPerDocument < result.MaxNumberOfOutputsPerDocument)
                combined.MaxNumberOfOutputsPerDocument = result.MaxNumberOfOutputsPerDocument;

            if (combined.LastQueryingTime < result.LastQueryingTime)
                combined.LastQueryingTime = result.LastQueryingTime;

            if (combined.LastIndexingTime < result.LastIndexingTime)
                combined.LastIndexingTime = result.LastIndexingTime;

            foreach (var collection in result.Collections)
            {
                combined.Collections[collection.Key] = null;
            }

            combined.LastBatchStats = null;
            combined.Memory = null;
        }

        private readonly struct ShardedIndexStatsOperation : IShardedOperation<IndexStats[]>
        {
            public IndexStats[] Combine(Memory<IndexStats[]> results)
            {
                if (results.Length == 0)
                    return null;

                var span = results.Span;
                var combined = span[0];

                for (var i = 1; i < span.Length; i++) // loop all shards (skip the first one)
                {
                    var result = span[i];

                    for (int j = 0; j < result.Length; j++)
                    {
                        FillIndexStats(combined[j], result[j]);
                    }
                }

                return combined;
            }

            public RavenCommand<IndexStats[]> CreateCommandForShard(int shard) => new GetIndexesStatisticsOperation.GetIndexesStatisticsCommand();
        }

    }
}
