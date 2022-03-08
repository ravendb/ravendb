using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Studio;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ShardedHandlers.Processors
{
    internal class ShardedStatsHandlerProcessorForGetStudioFooterStats : AbstractStatsHandlerProcessorForGetStudioFooterStats<ShardedRequestHandler, TransactionOperationContext>
    {
        public ShardedStatsHandlerProcessorForGetStudioFooterStats([NotNull] ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }
        
        protected override async ValueTask<FooterStatistics> GetFooterStatistics()
        {
            var op = new ShardedStudioStatsHandler.ShardedGetStudioFooterStatsOperation();
            var stats = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
            stats.CountOfIndexes = RequestHandler.ShardedContext.DatabaseRecord.Indexes.Count;
            //missing CountOfStaleIndexes and CountOfIndexingErrors fields
            return stats;
        }
    }
}
