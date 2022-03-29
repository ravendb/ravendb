using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Attachments
{
    internal class ShardedAttachmentsHandlerProcessorForDeleteAttachment : AbstractAttachmentsHandlerProcessorForDeleteAttachment<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAttachmentsHandlerProcessorForDeleteAttachment([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask DeleteAttachmentAsync(string docId, string name, LazyStringValue changeVector)
        {
            var cmd = new DeleteAttachmentOperation.DeleteAttachmentCommand(docId, name, changeVector);

            int shardNumber;
            using (RequestHandler.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
            }

            using (var token = RequestHandler.CreateOperationToken())
            {
                await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber, token.Token);
            }
        }
    }
}
