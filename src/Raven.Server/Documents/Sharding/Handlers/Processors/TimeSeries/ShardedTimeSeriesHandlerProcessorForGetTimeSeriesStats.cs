using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.TimeSeries
{
    internal class ShardedTimeSeriesHandlerProcessorForGetTimeSeriesStats : AbstractTimeSeriesHandlerProcessorForGetTimeSeriesStats<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedTimeSeriesHandlerProcessorForGetTimeSeriesStats([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask<TimeSeriesStatistics> GetTimeSeriesStatsAsync(string docId)
        {
            int shardNumber;
            using (RequestHandler.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
            }
            
            var op = new GetTimeSeriesStatisticsOperation.GetTimeSeriesStatisticsCommand(docId);
            return await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(op, shardNumber);
        }
    }
}
