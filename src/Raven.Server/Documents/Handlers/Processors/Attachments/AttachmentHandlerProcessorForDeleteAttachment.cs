using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal class AttachmentHandlerProcessorForDeleteAttachment : AbstractAttachmentHandlerProcessorForDeleteAttachment<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForDeleteAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask DeleteAttachmentAsync(DocumentsOperationContext _, string docId, string name, LazyStringValue changeVector)
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, docId, name, AttachmentType.Document, changeVector: null);
                if(attachment == null)
                    return;
                CheckAttachmentFlagAndThrowIfNeeded(docId, name, attachment);
            }

            var cmd = new AttachmentHandler.MergedDeleteAttachmentCommand
            {
                Database = RequestHandler.Database,
                ExpectedChangeVector = changeVector,
                DocumentId = docId,
                Name = name
            };
            await RequestHandler.Database.TxMerger.Enqueue(cmd);
            /* IF I am retired attachment
            I would like to use exising mechanizm of retiring attachment, to also delete it from cloud
            what I need to do, here: populate the tree or RetiredAttachments with some flag? 

            Then send MergedDeleteAttachmentCommand just to delete the attachment from attachments table


            */
            // here send a task to remove the attachment from cloud?
        }

        protected virtual void CheckAttachmentFlagAndThrowIfNeeded(string docId, string name, Attachment attachment)
        {
            if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
            {
                throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because it is retired. Please use dedicated API.");
            }
        }
    }
}
