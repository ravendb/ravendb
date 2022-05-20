using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Stats;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Stats;

internal class ShardedStatsHandlerProcessorForEssentialStats : AbstractStatsHandlerProcessorForEssentialStats<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedStatsHandlerProcessorForEssentialStats([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask<EssentialDatabaseStatistics> GetEssentialDatabaseStatisticsAsync(TransactionOperationContext context)
    {
        using (var token = RequestHandler.CreateOperationToken())
        {
            var stats = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new GetShardedEssentialStatisticsOperation(RequestHandler.HttpContext.Request), token.Token);

            var indexes = RequestHandler.DatabaseContext.Indexes.GetIndexes().ToList();

            stats.CountOfIndexes = indexes.Count;
            stats.Indexes = new BasicIndexInformation[indexes.Count];

            for (var i = 0; i < indexes.Count; i++)
            {
                var index = indexes[i];
                stats.Indexes[i] = index.ToBasicIndexInformation();
            }

            return stats;
        }
    }

    private struct GetShardedEssentialStatisticsOperation : IShardedOperation<EssentialDatabaseStatistics>
    {
        public GetShardedEssentialStatisticsOperation(HttpRequest request)
        {
            HttpRequest = request;
        }

        public HttpRequest HttpRequest { get; }

        public EssentialDatabaseStatistics Combine(Memory<EssentialDatabaseStatistics> results)
        {
            EssentialDatabaseStatistics result = null;

            foreach (var stats in results.Span)
            {
                if (result == null)
                {
                    result = stats;
                    continue;
                }

                MergeBasicDatabaseStatistics(result, stats);
            }

            Debug.Assert(result != null, "result != null");

            return result;
        }

        public RavenCommand<EssentialDatabaseStatistics> CreateCommandForShard(int shardNumber) => new GetEssentialStatisticsOperation.GetEssentialStatisticsCommand(debugTag: null);

        private static void MergeBasicDatabaseStatistics(EssentialDatabaseStatistics result, EssentialDatabaseStatistics stats)
        {
            result.CountOfConflicts += stats.CountOfConflicts;
            result.CountOfCounterEntries += stats.CountOfCounterEntries;
            result.CountOfDocuments += stats.CountOfDocuments;
            result.CountOfDocumentsConflicts += stats.CountOfDocumentsConflicts;

            result.CountOfRevisionDocuments += stats.CountOfRevisionDocuments;
            result.CountOfTimeSeriesSegments += stats.CountOfTimeSeriesSegments;
            result.CountOfTombstones += stats.CountOfTombstones;
            result.CountOfAttachments += stats.CountOfAttachments;
        }
    }
}
