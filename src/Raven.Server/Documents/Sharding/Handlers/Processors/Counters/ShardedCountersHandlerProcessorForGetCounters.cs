using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Documents.Handlers.Processors.Counters;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Counters
{
    internal class ShardedCountersHandlerProcessorForGetCounters : AbstractCountersHandlerProcessorForGetCounters<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedCountersHandlerProcessorForGetCounters([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask<CountersDetail> GetCountersAsync(string docId, StringValues counters, bool full)
        {
            var op = new GetCountersOperation.GetCounterValuesCommand(docId, counters, full);

            int shardNumber;
            using (RequestHandler.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
            }

            using (var token = RequestHandler.CreateOperationToken())
            {
                return await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(op, shardNumber, token.Token);
            }
        }
    }
}
