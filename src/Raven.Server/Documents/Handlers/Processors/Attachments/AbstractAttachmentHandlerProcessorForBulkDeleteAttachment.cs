using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal abstract class AbstractAttachmentHandlerProcessorForBulkDeleteAttachment<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractAttachmentHandlerProcessorForBulkDeleteAttachment([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask DeleteAttachmentAsync(List<AttachmentRequest> attachments);

        public override async ValueTask ExecuteAsync()
        {
            var attachmentRequests = new List<AttachmentRequest>();
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                using (var request = await context.ReadForDiskAsync(RequestHandler.RequestBodyStream(), "GetAttachments"))
                {
                    if (request.TryGet(nameof(GetAttachmentsOperation.GetAttachmentsCommand.Attachments), out BlittableJsonReaderArray attachments) == false)
                        throw new ArgumentException($"The '{nameof(GetAttachmentsOperation.GetAttachmentsCommand.Attachments)}' field in the body request is mandatory");

                    foreach (BlittableJsonReaderObject bjro in attachments)
                    {
                        if (bjro.TryGet(nameof(AttachmentRequest.DocumentId), out string docId) == false)
                            throw new ArgumentException($"Could not parse {nameof(AttachmentRequest.DocumentId)}");
                        if (bjro.TryGet(nameof(AttachmentRequest.Name), out string name) == false)
                            throw new ArgumentException($"Could not parse {nameof(AttachmentRequest.Name)}");

                        attachmentRequests.Add(new AttachmentRequest(docId, name));
                    }

                    if (attachmentRequests.Count == 0)
                        return;
                }
            }

            await DeleteAttachmentAsync(attachmentRequests);
        }
    }
}
