using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments;

internal sealed class AttachmentHandlerProcessorForHeadAttachment : AbstractAttachmentHandlerProcessorForHeadAttachment<DatabaseRequestHandler, DocumentsOperationContext>
{
    public AttachmentHandlerProcessorForHeadAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ValueTask HandleHeadAttachmentAsync(string documentId, string name, string changeVector)
    {
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        using (context.OpenReadTransaction())
        {
            var attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, documentId, name, AttachmentType.Document, null);
            if (attachment == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return ValueTask.CompletedTask;
            }

            if (changeVector == attachment.ChangeVector)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return ValueTask.CompletedTask;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = $"\"{attachment.ChangeVector}\"";

            RangeHelper.SetRangeHeaders(HttpContext, attachment.Size);

            return ValueTask.CompletedTask;
        }
    }
}
