using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired
{
    internal sealed class RetiredAttachmentHandlerProcessorForDelete : AttachmentHandlerProcessorForDeleteAttachment
    {
        public RetiredAttachmentHandlerProcessorForDelete([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override AttachmentHandler.MergedDeleteAttachmentCommand CreateMergedDeleteAttachmentCommand(string docId, string name, LazyStringValue changeVector)
        {
            var storageOnly = RequestHandler.GetBoolValueQueryString("storageOnly", required: false) ?? false;
            var cmd = base.CreateMergedDeleteAttachmentCommand(docId, name, changeVector);
            cmd.StorageOnly = storageOnly;

            return cmd;
        }

        protected override void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, string docId, string name)
        {
            using (context.OpenReadTransaction())
            {
                CheckRetiredAttachmentFlagAndThrowIfNeededInternal(context, RequestHandler, docId, name);
            }
        }

        public static void CheckRetiredAttachmentFlagAndThrowIfNeededInternal(DocumentsOperationContext context, DatabaseRequestHandler requestHandler, string docId,
            string name)
        {

            Attachment attachment = requestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, docId, name, AttachmentType.Document, changeVector: null);
            if (attachment == null)
                return;

            if (attachment.Flags.HasFlag(AttachmentFlags.Retired) == false)
            {
                throw new InvalidOperationException($"Cannot delete retired attachment '{name}' on document '{docId}' because it is not retired. Please use dedicated Client API.");
            }

            var dbRecord = requestHandler.Database.ReadDatabaseRecord();

            if (dbRecord.RetireAttachments == null)
            {
                throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because {nameof(RetireAttachmentsConfiguration)} is not configured.");
            }

            if (dbRecord.RetireAttachments.Disabled)
            {
                throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because {nameof(RetireAttachmentsConfiguration)} is disabled.");
            }

            if (dbRecord.RetireAttachments.HasUploader() == false)
            {
                throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because {nameof(RetireAttachmentsConfiguration)} does not have any uploader configured.");
            }
        }
    }
}
