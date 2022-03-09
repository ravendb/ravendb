using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.ShardedHandlers;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Processors
{
    internal class ShardedStatsHandlerProcessorForGetDatabaseStatistics : AbstractStatsHandlerProcessorForGetDatabaseStatistics <ShardedRequestHandler, TransactionOperationContext>
    {
        public ShardedStatsHandlerProcessorForGetDatabaseStatistics([NotNull] ShardedRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async Task<DatabaseStatistics> GetDatabaseStatistics()
        {
            var op = new ShardedStatsHandler.ShardedStatsOperation();

            var stats = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
            stats.Indexes = GetDatabaseIndexesFromRecord();
            stats.CountOfIndexes = stats.Indexes.Length;

            return stats;
        }
    }
}
