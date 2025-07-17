using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal class AttachmentHandlerProcessorForBulkDeleteAttachment : AbstractAttachmentHandlerProcessorForBulkDeleteAttachment<DatabaseRequestHandler,
        DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForBulkDeleteAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask DeleteAttachmentAsync(DocumentsOperationContext context, BlittableJsonReaderArray attachments, OperationCancelToken operationCancelToken)
        {
            var attachmentRequests = new List<AttachmentRequest>();
            using (context.OpenReadTransaction())
            {
                foreach (BlittableJsonReaderObject bjro in attachments)
                {
                    using (bjro)
                    {
                        if (bjro.TryGet(nameof(AttachmentRequest.DocumentId), out string docId) == false)
                            throw new ArgumentException($"Could not parse {nameof(AttachmentRequest.DocumentId)}");
                        if (bjro.TryGet(nameof(AttachmentRequest.Name), out string name) == false)
                            throw new ArgumentException($"Could not parse {nameof(AttachmentRequest.Name)}");

                        var attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, docId, name, AttachmentType.Document, changeVector: null);
                        if (attachment == null)
                            continue;

                        attachmentRequests.Add(new AttachmentRequest(docId, name));
                    }
                }
            }

            if (attachmentRequests.Count == 0)
                return;

            MergedDeleteAttachmentsCommand cmd = new MergedDeleteAttachmentsCommand
            {
                Database = RequestHandler.Database,
                Deletes = attachmentRequests
            };

            await RequestHandler.Database.TxMerger.Enqueue(cmd);
        }

    }

}
