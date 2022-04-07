using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal class AttachmentHandlerProcessorForGetAttachment : AbstractAttachmentHandlerProcessorForGetAttachment<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForGetAttachment([NotNull] DatabaseRequestHandler requestHandler, bool isDocument) : base(requestHandler, requestHandler.ContextPool, requestHandler.Logger, isDocument)
        {
        }

        protected override ValueTask<AttachmentResult> GetAttachmentAsync(DocumentsOperationContext context, string documentId, string name, AttachmentType type, string changeVector, CancellationToken _)
        {
            var attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, documentId, name, type, changeVector);
            
            if (attachment == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return ValueTask.FromResult<AttachmentResult>(null);
            }

            var attachmentChangeVector = RequestHandler.GetStringFromHeaders("If-None-Match");
            if (attachmentChangeVector == attachment.ChangeVector)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return ValueTask.FromResult<AttachmentResult>(null);
            }

            AttachmentResult attachmentResult = new()
            {
                Details = new AttachmentDetails()
                {
                    DocumentId = documentId,
                    ChangeVector = attachment.ChangeVector,
                    Hash = attachment.Base64Hash.ToString(),
                    ContentType = attachment.ContentType,
                    Name = attachment.Name,
                    Size = attachment.Size
                },
                Stream = attachment.Stream
            };

            return ValueTask.FromResult(attachmentResult);
        }

        protected override RavenTransaction OpenReadTransaction(DocumentsOperationContext context)
        {
            return context.OpenReadTransaction();
        }
    }
}
