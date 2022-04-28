using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Session.Operations;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedRevisionsHandlerProcessorForGetRevisionsCount : AbstractRevisionsHandlerProcessorForGetRevisionsCount<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedRevisionsHandlerProcessorForGetRevisionsCount([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<GetRevisionsCountOperation.DocumentRevisionsCount> GetRevisionsCountAsync(string docId)
        {
            int shardNumber;
            using (RequestHandler.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using(context.OpenReadTransaction())
            {
                shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
            }

            long count;
            using (var token = RequestHandler.CreateOperationToken())
            {
                var op = new GetRevisionsCountOperation.GetRevisionsCountCommand(docId);
                count = await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(op, shardNumber, token.Token);
            }

            return new GetRevisionsCountOperation.DocumentRevisionsCount()
            {
                RevisionsCount = count
            };
        }
    }
}
