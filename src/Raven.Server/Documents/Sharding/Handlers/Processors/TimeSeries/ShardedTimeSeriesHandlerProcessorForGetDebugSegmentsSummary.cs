using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.TimeSeries
{
    internal class ShardedTimeSeriesHandlerProcessorForGetDebugSegmentsSummary : AbstractTimeSeriesHandlerProcessorForGetDebugSegmentsSummary<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedTimeSeriesHandlerProcessorForGetDebugSegmentsSummary([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask GetSegmentsSummaryAndWriteAsync(TransactionOperationContext context, string docId, string name, DateTime @from, DateTime to)
        {
            int shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
            var op = new ProxyCommand<SegmentsSummary>(new GetSegmentsSummaryOperation.GetSegmentsSummaryCommand(docId, name, from, to), RequestHandler.HttpContext.Response);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(op, shardNumber, RequestHandler.CreateOperationToken().Token);
        }
    }
}
