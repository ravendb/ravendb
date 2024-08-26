using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired
{
    internal class RetiredAttachmentHandlerProcessorForBulkDelete : AttachmentHandlerProcessorForBulkDeleteAttachment
    {
        public RetiredAttachmentHandlerProcessorForBulkDelete([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override void CheckAttachmentFlagAndThrowIfNeeded(DocumentsOperationContext context, string docId, string name)
        {
            RetiredAttachmentHandlerProcessorForDelete.CheckRetiredAttachmentFlagAndThrowIfNeededInternal(context, RequestHandler, docId, name);
        }
    }
}
