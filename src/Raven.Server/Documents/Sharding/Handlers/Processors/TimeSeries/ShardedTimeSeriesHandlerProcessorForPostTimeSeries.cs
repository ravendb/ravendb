using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.TimeSeries
{
    internal sealed class ShardedTimeSeriesHandlerProcessorForPostTimeSeries : AbstractTimeSeriesHandlerProcessorForPostTimeSeries<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedTimeSeriesHandlerProcessorForPostTimeSeries([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask ApplyTimeSeriesOperationAsync(string docId, TimeSeriesOperation operation, TransactionOperationContext context)
        {
            int shardNumber = RequestHandler.DatabaseContext.GetShardNumberFor(context, docId);
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                var cmd = new TimeSeriesBatchOperation.TimeSeriesBatchCommand(RequestHandler.ShardExecutor.Conventions, docId, operation);
                await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(new ProxyCommand<object>(cmd, RequestHandler.HttpContext.Response), shardNumber, token.Token);
            }
        }
    }
}
