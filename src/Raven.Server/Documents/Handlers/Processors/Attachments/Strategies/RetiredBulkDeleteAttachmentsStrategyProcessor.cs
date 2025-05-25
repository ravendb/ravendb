using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Strategies
{
    internal class RetiredBulkDeleteAttachmentsStrategyProcessor : AbstractBulkDeleteAttachmentsStrategyProcessor<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RetiredBulkDeleteAttachmentsStrategyProcessor([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override MergedDeleteRetiredAttachmentsCommand MergedDeleteAttachmentsCommand(List<AttachmentRequest> attachmentRequests)
        {
            var storageOnly = RequestHandler.GetBoolValueQueryString("storageOnly", required: false) ?? false;
            var cmd = new MergedDeleteRetiredAttachmentsCommand
            {
                Database = RequestHandler.Database,
                Deletes = attachmentRequests,
                DeleteState = storageOnly ? AttachmentsStorage.DeleteAttachmentState.DocumentRetiredAttachmentStorage : AttachmentsStorage.DeleteAttachmentState.DocumentRetiredAttachmentCloudStorage
            };
            return cmd;
        }

        public override void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context,Attachment attachment, string docId, string name)
        {
            RetiredDeleteAttachmentStrategyProcessor.CheckRetiredAttachmentFlagAndThrowIfNeededInternal(context, attachment, RequestHandler, docId, name);
        }
    }
}
