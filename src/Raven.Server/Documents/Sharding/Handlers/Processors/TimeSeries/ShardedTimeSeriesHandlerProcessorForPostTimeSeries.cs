using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.TimeSeries
{
    internal class ShardedTimeSeriesHandlerProcessorForPostTimeSeries : AbstractTimeSeriesHandlerProcessorForPostTimeSeries<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedTimeSeriesHandlerProcessorForPostTimeSeries([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask ApplyTimeSeriesOperationAsync(string docId, TimeSeriesOperation operation, TransactionOperationContext context)
        {
            int shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
            using (var token = RequestHandler.CreateOperationToken())
            {
                var op = new TimeSeriesBatchOperation.TimeSeriesBatchCommand(docId, operation);
                await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(op, shardNumber, token.Token);
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Handle status codes. RavenDB-18416");
            }
        }
    }
}
