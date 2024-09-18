using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide;
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

        protected abstract ValueTask DeleteAttachmentAsync(TOperationContext context, BlittableJsonReaderArray attachments, OperationCancelToken operationCancelToken);

        public override async ValueTask ExecuteAsync()
        {

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using (var operationCancelToken = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                using var request = await context.ReadForDiskAsync(RequestHandler.RequestBodyStream(), "GetAttachments");

                //if (request.TryGet(nameof(AttachmentType), out string typeString) == false || Enum.TryParse(typeString, out AttachmentType type) == false)
                //    throw new ArgumentException($"The '{nameof(AttachmentType)}' field in the body request is mandatory");

                if (request.TryGet(nameof(GetAttachmentsOperation.GetAttachmentsCommand.Attachments), out BlittableJsonReaderArray attachments) == false)
                    throw new ArgumentException($"The '{nameof(GetAttachmentsOperation.GetAttachmentsCommand.Attachments)}' field in the body request is mandatory");

                await DeleteAttachmentAsync(context, attachments, operationCancelToken);
            }
        }

        //protected static void WriteAttachmentDetails(AsyncBlittableJsonTextWriter writer, Attachment attachment, string documentId)
        //{
        //    writer.WriteStartObject();
        //    writer.WritePropertyName(nameof(AttachmentDetails.Name));
        //    writer.WriteString(attachment.Name);
        //    writer.WriteComma();
        //    writer.WritePropertyName(nameof(AttachmentDetails.Hash));
        //    writer.WriteString(attachment.Base64Hash.ToString());
        //    writer.WriteComma();
        //    writer.WritePropertyName(nameof(AttachmentDetails.ContentType));
        //    writer.WriteString(attachment.ContentType);
        //    writer.WriteComma();
        //    writer.WritePropertyName(nameof(AttachmentDetails.Size));
        //    writer.WriteInteger(attachment.Size);
        //    writer.WriteComma();
        //    writer.WritePropertyName(nameof(AttachmentDetails.ChangeVector));
        //    writer.WriteString(attachment.ChangeVector);
        //    writer.WriteComma();
        //    writer.WritePropertyName(nameof(AttachmentDetails.DocumentId));
        //    writer.WriteString(documentId);
        //    writer.WriteEndObject();
        //}
    }
}
