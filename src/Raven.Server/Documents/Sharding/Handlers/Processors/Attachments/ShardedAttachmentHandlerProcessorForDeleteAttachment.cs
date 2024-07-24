using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Server.Documents.Handlers.Processors.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Attachments
{
    internal sealed class ShardedAttachmentHandlerProcessorForDeleteAttachment : AbstractAttachmentHandlerProcessorForDeleteAttachment<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAttachmentHandlerProcessorForDeleteAttachment([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask DeleteAttachmentAsync(TransactionOperationContext context, string docId, string name, LazyStringValue changeVector)
        {
            var cmd = new DeleteAttachmentOperation.DeleteAttachmentCommand(docId, name, changeVector);

            int shardNumber = RequestHandler.DatabaseContext.GetShardNumberFor(context, docId);
            
            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber, token.Token);
            }
        }
    }
}
