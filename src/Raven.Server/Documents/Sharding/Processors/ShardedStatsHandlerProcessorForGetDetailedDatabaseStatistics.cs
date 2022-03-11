using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Processors
{
    internal class ShardedStatsHandlerProcessorForGetDetailedDatabaseStatistics : AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics<ShardedRequestHandler, TransactionOperationContext>
    {
        public ShardedStatsHandlerProcessorForGetDetailedDatabaseStatistics([NotNull] ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override string GetDatabaseName()
        {
            return RequestHandler.ShardedContext.DatabaseName;
        }

        protected override async ValueTask<DetailedDatabaseStatistics> GetDatabaseStatisticsAsync()
        {
            var operation = new ShardedDetailedStatsOperation();

            var detailedStatistics = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(operation);
            detailedStatistics.Indexes = ShardedStatsHandlerProcessorForGetDatabaseStatistics.GetDatabaseIndexesFromRecord(RequestHandler.ShardedContext.DatabaseRecord);
            detailedStatistics.CountOfIndexes = detailedStatistics.Indexes.Length;

            return detailedStatistics;
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
                    ShardedStatsHandlerProcessorForGetDatabaseStatistics.ShardedStatsOperation.FillDatabaseStatistics(combined, result, ref totalSizeOnDisk, ref totalTempBuffersSizeOnDisk);
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
