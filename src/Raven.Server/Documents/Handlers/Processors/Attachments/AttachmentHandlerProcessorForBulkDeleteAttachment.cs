using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal sealed class AttachmentHandlerProcessorForBulkDeleteAttachment : AbstractAttachmentHandlerProcessorForBulkDeleteAttachment<DatabaseRequestHandler, DocumentsOperationContext>
    {
        internal AttachmentHandlerProcessorForBulkDeleteAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask DeleteAttachmentAsync(List<AttachmentRequest> attachments)
        {
            MergedDeleteAttachmentsCommand cmd = new MergedDeleteAttachmentsCommand
            {
                Database = RequestHandler.Database,
                Deletes = attachments
            };

            await RequestHandler.Database.TxMerger.Enqueue(cmd);
        }
    }
}
