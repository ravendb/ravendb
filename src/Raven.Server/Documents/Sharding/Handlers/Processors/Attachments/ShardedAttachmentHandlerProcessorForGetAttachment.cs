using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Attachments
{
    internal class ShardedAttachmentHandlerProcessorForGetAttachment : AbstractAttachmentHandlerProcessorForGetAttachment<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAttachmentHandlerProcessorForGetAttachment([NotNull] ShardedDatabaseRequestHandler requestHandler, bool isDocument) : base(requestHandler, requestHandler.ContextPool, requestHandler.Logger, isDocument)
        {
        }

        protected override async ValueTask GetAttachmentAsync(TransactionOperationContext context, string documentId, string name, AttachmentType type, string changeVector, CancellationToken token)
        {
            int shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, documentId);
            var cmd = new GetAttachmentOperation.GetAttachmentCommand(context, documentId, name, type, changeVector);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(new  ProxyCommand<AttachmentResult>(cmd, RequestHandler.HttpContext.Response), shardNumber, token);
        }
        
        protected override RavenTransaction OpenReadTransaction(TransactionOperationContext context)
        {
            return context.OpenReadTransaction();
        }
    }
}
