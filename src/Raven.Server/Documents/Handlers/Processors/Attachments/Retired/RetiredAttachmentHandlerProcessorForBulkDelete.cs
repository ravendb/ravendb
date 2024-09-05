using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired
{
    internal class RetiredAttachmentHandlerProcessorForBulkDelete : AttachmentHandlerProcessorForBulkDeleteAttachment
    {
        public RetiredAttachmentHandlerProcessorForBulkDelete([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override MergedDeleteAttachmentsCommand MergedDeleteAttachmentsCommand(List<AttachmentRequest> attachmentRequests)
        {
            var cmd = base.MergedDeleteAttachmentsCommand(attachmentRequests);
            var storageOnly = RequestHandler.GetBoolValueQueryString("storageOnly", required: false) ?? false;

            cmd.StorageOnly = storageOnly;
            return cmd;
        }

        protected override void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, string docId, string name)
        {
            RetiredAttachmentHandlerProcessorForDelete.CheckRetiredAttachmentFlagAndThrowIfNeededInternal(context, RequestHandler, docId, name);
        }
    }
}
