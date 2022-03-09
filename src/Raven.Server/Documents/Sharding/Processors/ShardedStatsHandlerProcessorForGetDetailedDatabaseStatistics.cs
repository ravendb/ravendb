using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.ShardedHandlers;
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

        protected override async Task<DetailedDatabaseStatistics> GetDatabaseStatistics()
        {
            var op = new ShardedStatsHandler.ShardedDetailedStatsOperation();

            var detailedStatistics = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
            detailedStatistics.Indexes = GetDatabaseIndexesFromRecord();
            detailedStatistics.CountOfIndexes = detailedStatistics.Indexes.Length;

            return detailedStatistics;
        }
    }
}
