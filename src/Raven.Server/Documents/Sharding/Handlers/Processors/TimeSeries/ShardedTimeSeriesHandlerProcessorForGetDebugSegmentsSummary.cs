using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.TimeSeries
{
    internal class ShardedTimeSeriesHandlerProcessorForGetDebugSegmentsSummary : AbstractTimeSeriesHandlerProcessorForGetDebugSegmentsSummary<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedTimeSeriesHandlerProcessorForGetDebugSegmentsSummary([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetSegmentsSummaryAndWriteAsync(TransactionOperationContext context, string docId, string name, DateTime @from, DateTime to)
        {
            int shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
            var op = new ProxyCommand<SegmentsSummary>(new GetSegmentsSummaryOperation.GetSegmentsSummaryCommand(docId, name, from, to), RequestHandler.HttpContext.Response);
            using(var token = RequestHandler.CreateOperationToken())
                await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(op, shardNumber, token.Token);
        }
    }
}
