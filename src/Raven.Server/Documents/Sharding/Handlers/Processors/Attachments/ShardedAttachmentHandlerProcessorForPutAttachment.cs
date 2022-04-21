using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Attachments
{
    internal class ShardedAttachmentHandlerProcessorForPutAttachment : AbstractAttachmentHandlerProcessorForPutAttachment<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAttachmentHandlerProcessorForPutAttachment([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask PutAttachmentsAsync(TransactionOperationContext context, string id, string name, Stream requestBodyStream, string contentType, string changeVector, CancellationToken token)
        {
            int shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, id);
            var op = new PutAttachmentOperation.PutAttachmentCommand(id, name, requestBodyStream, contentType, changeVector, validateStream: false);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(new ProxyCommand<AttachmentDetails>(op, RequestHandler.HttpContext.Response), shardNumber, token);
        }
    }
}
