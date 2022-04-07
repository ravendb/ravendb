using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Attachments
{
    internal class ShardedAttachmentHandlerProcessorForGetAttachment : AbstractAttachmentHandlerProcessorForGetAttachment<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAttachmentHandlerProcessorForGetAttachment([NotNull] ShardedDatabaseRequestHandler requestHandler, bool isDocument) : base(requestHandler, requestHandler.ContextPool, requestHandler.Logger, isDocument)
        {
        }

        protected override async ValueTask<AttachmentResult> GetAttachmentAsync(TransactionOperationContext context, string documentId, string name, AttachmentType type, string changeVector)
        {
            int shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, documentId);
            var cmd = new GetAttachmentOperation.GetAttachmentCommand(context, documentId, name, type, changeVector);
            using (var token = RequestHandler.CreateOperationToken())
            {
                var result = await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber, token.Token);
                HttpContext.Response.StatusCode = (int)cmd.StatusCode;
                return result;
            }
        }

        protected override CancellationToken GetDataBaseShutDownToken()
        {
            return RequestHandler.DatabaseContext.DatabaseShutdown;
        }

        protected override RavenTransaction OpenReadTransaction(TransactionOperationContext context)
        {
            return context.OpenReadTransaction();
        }
    }
}
