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

        protected override async ValueTask DeleteAttachmentAsync(DocumentsOperationContext context, string docId, string name, LazyStringValue changeVector)
        {
            CheckAttachmentFlagAndThrowIfNeeded(context, docId, name);

            var cmd = CreateMergedDeleteAttachmentCommand(docId, name, changeVector);
            await RequestHandler.Database.TxMerger.Enqueue(cmd);
        }

        protected virtual AttachmentHandler.MergedDeleteAttachmentCommand CreateMergedDeleteAttachmentCommand(string docId, string name, LazyStringValue changeVector)
        {
            var cmd = new AttachmentHandler.MergedDeleteAttachmentCommand
            {
                Database = RequestHandler.Database,
                ExpectedChangeVector = changeVector,
                DocumentId = docId,
                Name = name
            };
            return cmd;
        }

        protected virtual void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, string docId, string name)
        {
            using (context.OpenReadTransaction())
            {
                CheckAttachmentFlagAndThrowIfNeededInternal(context, RequestHandler, docId, name);
            }
        }

        public static void CheckAttachmentFlagAndThrowIfNeededInternal(DocumentsOperationContext context, DatabaseRequestHandler requestHandler, string docId, string name)
        {
            //TODO: egor I have CV, do I need to pass it here? CHeck in future (if test pass when I add it)
            //TODO: egor shouldn't Attachment be IDisposable? it has lsv & Stream that should be disposed
            //TODO: egor sharding handler?
            var attachment = requestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, docId, name, AttachmentType.Document, changeVector: null);
            if (attachment == null)
                return;
            using var _ = attachment.Stream;

            if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
            {
                throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because it is retired. Please use dedicated API.");
            }
        }
    }
}
