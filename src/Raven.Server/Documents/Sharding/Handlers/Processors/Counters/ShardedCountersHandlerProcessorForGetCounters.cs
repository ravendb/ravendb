using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Handlers.Processors.Counters;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Counters
{
    internal sealed class ShardedCountersHandlerProcessorForGetCounters : AbstractCountersHandlerProcessorForGetCounters<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedCountersHandlerProcessorForGetCounters([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<CountersDetail> GetCountersAsync(TransactionOperationContext context, string docId, StringValues counters, bool full)
        {
            var op = new GetCountersOperation.GetCounterValuesCommand(RequestHandler.ShardExecutor.Conventions, docId, counters, full);

            var shardNumber = RequestHandler.DatabaseContext.GetShardNumberFor(context, docId);

            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                return await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(op, shardNumber, token.Token);
            }
        }
    }
}
