using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments.Retired
{
    internal class RetiredAttachmentHandlerProcessorForHead : AttachmentHandlerProcessorForHeadAttachment
    {
        public RetiredAttachmentHandlerProcessorForHead([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {

        }

        public override string CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            return RetiredAttachmentHandlerProcessorForGet.CheckAttachmentFlagAndConfigurationAndThrowIfNeededInternal(context, RequestHandler.Database, attachment, documentId, name, "head");
        }
    }
}
