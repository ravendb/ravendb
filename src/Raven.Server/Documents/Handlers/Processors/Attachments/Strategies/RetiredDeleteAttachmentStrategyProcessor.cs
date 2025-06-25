using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal class RetiredDeleteAttachmentStrategyProcessor : AbstractDeleteAttachmentStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RetiredDeleteAttachmentStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override AttachmentHandler.MergedDeleteRetiredAttachmentCommand CreateMergedDeleteAttachmentCommand(string docId, string name, LazyStringValue changeVector)
        {
            //TODO: egor not retired attachment will be always storageOnly = true 
            var storageOnly = RequestHandler.GetBoolValueQueryString("storageOnly", required: false) ?? false;

            var cmd = new AttachmentHandler.MergedDeleteRetiredAttachmentCommand
            {
                Database = RequestHandler.Database,
                ExpectedChangeVector = changeVector,
                DocumentId = docId,
                Name = name,
                DeleteState = storageOnly ? AttachmentsStorage.DeleteAttachmentState.DocumentRetiredAttachmentStorage : AttachmentsStorage.DeleteAttachmentState.DocumentRetiredAttachmentCloudStorage

            };
            return cmd;
        }

        public override void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string docId, string name)
        {
            CheckRetiredAttachmentFlagAndThrowIfNeededInternal(context, attachment, RequestHandler, docId, name);
        }

        public static void CheckRetiredAttachmentFlagAndThrowIfNeededInternal(DocumentsOperationContext context, Attachment attachment, DatabaseRequestHandler requestHandler, string docId,
            string name)
        {
            if (attachment == null)
                return;

            if (attachment.Flags.HasFlag(AttachmentFlags.Retired) == false)
            {
                throw new InvalidOperationException($"Cannot delete retired attachment '{name}' on document '{docId}' because it is not retired. Please use dedicated Client API.");
            }

            var dbRecord = requestHandler.Database.ReadDatabaseRecord();

            if (dbRecord.RetiredAttachments == null)
            {
                throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because {nameof(RetiredAttachmentsConfiguration)} is not configured.");
            }

            if (dbRecord.RetiredAttachments.Disabled)
            {
                throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because {nameof(RetiredAttachmentsConfiguration)} is disabled.");
            }

            if (dbRecord.RetiredAttachments.HasUploader() == false)
            {
                throw new InvalidOperationException($"Cannot delete attachment '{name}' on document '{docId}' because {nameof(RetiredAttachmentsConfiguration)} does not have any uploader configured.");
            }
        }
    }
}
