using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Revisions
{
    internal class ShardedRevisionsHandlerProcessorForDeleteRevisions : AbstractRevisionsHandlerProcessorForDeleteRevisions<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedRevisionsHandlerProcessorForDeleteRevisions([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async Task<long> DeleteRevisionsAsync(DeleteRevisionsRequest request, OperationCancelToken token)
        {
            var cmd = new DeleteRevisionsManuallyCommand(request);
            int shardNumber;

            var config = RequestHandler.DatabaseContext.DatabaseRecord.Sharding;
            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                shardNumber = ShardHelper.GetShardNumberFor(config, context, request.DocumentId);
            }

            var result = await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber, token.Token);
            return result.TotalDeletes;
        }
    }
}
