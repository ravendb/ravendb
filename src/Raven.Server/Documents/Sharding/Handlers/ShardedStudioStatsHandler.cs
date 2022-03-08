extern alias NGC;
using System;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.ShardedHandlers.Processors;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Studio;
using Raven.Server.Routing;

namespace Raven.Server.Documents.ShardedHandlers
{
    internal class ShardedStudioStatsHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/studio/footer/stats", "GET")]
        public async Task FooterStats()
        {
            using (var processor = new ShardedStatsHandlerProcessorForGetStudioFooterStats(this))
            {
                await processor.ExecuteAsync();
            }
        }

        public readonly struct ShardedGetStudioFooterStatsOperation : IShardedOperation<FooterStatistics>
        {
            public FooterStatistics Combine(Memory<FooterStatistics> results)
            {
                var span = results.Span;
                
                var combined = new FooterStatistics();
                
                foreach (var stats in span)
                {
                    combined.CountOfDocuments += stats.CountOfDocuments;
                }
                
                return combined;
            }

            public RavenCommand<FooterStatistics> CreateCommandForShard(int shard) => new GetStudioFooterStatisticsOperation.GetStudioFooterStatisticsCommand();
        }
    }
}
