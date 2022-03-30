using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal class AttachmentsHandlerProcessorForDeleteAttachment : AbstractAttachmentsHandlerProcessorForDeleteAttachment<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AttachmentsHandlerProcessorForDeleteAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask DeleteAttachmentAsync(DocumentsOperationContext _, string docId, string name, LazyStringValue changeVector)
        {
            var cmd = new AttachmentHandler.MergedDeleteAttachmentCommand
            {
                Database = RequestHandler.Database,
                ExpectedChangeVector = changeVector,
                DocumentId = docId,
                Name = name
            };
            await RequestHandler.Database.TxMerger.Enqueue(cmd);
        }
    }
}
