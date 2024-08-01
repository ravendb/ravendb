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
            CheckAttachmentFlagAndThrowIfNeeded(docId, name);

            var cmd = new AttachmentHandler.MergedDeleteAttachmentCommand
            {
                Database = RequestHandler.Database,
                ExpectedChangeVector = changeVector,
                DocumentId = docId,
                Name = name
            };
            await RequestHandler.Database.TxMerger.Enqueue(cmd);
        }

        protected virtual void CheckAttachmentFlagAndThrowIfNeeded(string docId, string name)
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, docId, name, AttachmentType.Document, changeVector: null);
                if (attachment == null)
                    return;

                if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
                {
                    throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because it is retired. Please use dedicated API.");
                }
            }
        }
    }
}
