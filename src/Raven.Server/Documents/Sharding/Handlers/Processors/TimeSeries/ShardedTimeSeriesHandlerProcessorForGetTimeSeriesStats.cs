using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.TimeSeries
{
    internal class ShardedTimeSeriesHandlerProcessorForGetTimeSeriesStats : AbstractTimeSeriesHandlerProcessorForGetTimeSeriesStats<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedTimeSeriesHandlerProcessorForGetTimeSeriesStats([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetTimeSeriesStatsAndWriteAsync(TransactionOperationContext context, string docId)
        {
            int shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
            var op = new GetTimeSeriesStatisticsOperation.GetTimeSeriesStatisticsCommand(docId);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(new ProxyCommand<TimeSeriesStatistics>(op, RequestHandler.HttpContext.Response), shardNumber);
        }
    }
}
