using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired
{
    internal sealed class RetiredAttachmentHandlerProcessorForDeleteRetiredAttachment : AttachmentHandlerProcessorForDeleteAttachment
    {
        public RetiredAttachmentHandlerProcessorForDeleteRetiredAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void CheckAttachmentFlagAndThrowIfNeeded(string docId, string name, Attachment attachment)
        {
            if (attachment.Flags.HasFlag(AttachmentFlags.Retired) == false)
            {
                throw new InvalidOperationException($"Cannot delete retired attachment '{name}' on document '{docId}' because it is not retired. Please use dedicated Client API.");
            }

            var dbRecord = RequestHandler.Database.ReadDatabaseRecord();

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
