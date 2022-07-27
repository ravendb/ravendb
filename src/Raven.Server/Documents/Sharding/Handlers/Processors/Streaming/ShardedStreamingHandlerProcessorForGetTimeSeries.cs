using System;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Commands.TimeSeries;
using Raven.Server.Documents.Handlers.Processors.Streaming;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Streaming
{
    internal class ShardedStreamingHandlerProcessorForGetTimeSeries : AbstractStreamingHandlerProcessorForGetTimeSeries<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStreamingHandlerProcessorForGetTimeSeries([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override IDisposable OpenReadTransaction(TransactionOperationContext context)
        {
            return context.OpenReadTransaction();
        }

        protected override async ValueTask GetAndWriteTimeSeriesAsync(TransactionOperationContext context, string docId, string name, DateTime @from, DateTime to, TimeSpan? offset,
            CancellationToken token)
        {
            var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
            var cmd = new GetTimeSeriesCommand(docId, name, from, to, offset);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(new ProxyCommand(cmd, HttpContext.Response), shardNumber, token);
        }
    }
}
