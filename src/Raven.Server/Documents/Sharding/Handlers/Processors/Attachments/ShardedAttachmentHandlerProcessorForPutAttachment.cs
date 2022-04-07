using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Attachments
{
    internal class ShardedAttachmentHandlerProcessorForPutAttachment : AbstractAttachmentHandlerProcessorForPutAttachment<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedAttachmentHandlerProcessorForPutAttachment([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask<AttachmentDetails> PutAttachmentsAsync(TransactionOperationContext context, string id, string name, Stream requestBodyStream, string contentType, string changeVector, CancellationToken token)
        {
            int shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, id);
            var op = new PutAttachmentOperation.PutAttachmentCommand(id, name, requestBodyStream, contentType, changeVector, validateStream: false);
        
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal,
                "Pass NotModified/NotFound status code and Etag headers. RavenDB-18416.");
            
            var result = await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(op, shardNumber, token);
            HttpContext.Response.StatusCode = (int)op.StatusCode;
            return result;
        }
    }
}
