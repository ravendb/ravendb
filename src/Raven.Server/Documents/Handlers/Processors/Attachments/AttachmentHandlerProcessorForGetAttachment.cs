using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal class AttachmentHandlerProcessorForGetAttachment : AbstractAttachmentHandlerProcessorForGetAttachment<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForGetAttachment([NotNull] DatabaseRequestHandler requestHandler, bool isDocument) : base(requestHandler, isDocument)
        {
        }

        protected override async ValueTask GetAttachmentAsync(DocumentsOperationContext context, string documentId, string name, AttachmentType type, string changeVector, CancellationToken token)
        {
            using (var tx = context.OpenReadTransaction())
            {
                var attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, documentId, name, type, changeVector);

                if (attachment == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var collection = CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(context, attachment, documentId, name);

                var attachmentChangeVector = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);
                if (attachmentChangeVector == attachment.ChangeVector)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                try
                {
                    var fileName = Path.GetFileName(attachment.Name);
                    fileName = Uri.EscapeDataString(fileName);
                    HttpContext.Response.Headers[Constants.Headers.ContentDisposition] = $"attachment; filename=\"{fileName}\"; filename*=UTF-8''{fileName}";
                }
                catch (ArgumentException e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Skip Content-Disposition header because of not valid file name: {attachment.Name}", e);
                }

                try
                {
                    HttpContext.Response.Headers[Constants.Headers.ContentType] = attachment.ContentType.ToString();
                }
                catch (InvalidOperationException e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Skip Content-Type header because of not valid content type: {attachment.ContentType}", e);
                    if (HttpContext.Response.Headers.ContainsKey(Constants.Headers.ContentType))
                        HttpContext.Response.Headers.Remove(Constants.Headers.ContentType);
                }

                HttpContext.Response.Headers[Constants.Headers.AttachmentHash] = attachment.Base64Hash.ToString();
                HttpContext.Response.Headers[Constants.Headers.AttachmentSize] = attachment.Size.ToString();
                HttpContext.Response.Headers[Constants.Headers.Etag] = $"\"{attachment.ChangeVector}\"";
                HttpContext.Response.Headers[Constants.Headers.AttachmentRetireAt] = attachment.RetiredAt?.GetDefaultRavenFormat();
                HttpContext.Response.Headers[Constants.Headers.AttachmentFlags] = ((int)attachment.Flags).ToString();
         //       HttpContext.Response.Headers[Constants.Headers.AttachmentCollection] = attachment.Collection.ToString();
                DisposeReadTransactionIfNeeded(tx);

                await WriteResponseStream(context, attachment, collection, token);
            }
        }

        public virtual void DisposeReadTransactionIfNeeded(DocumentsTransaction tx)
        {
            // noop
        }

        public virtual string CheckAttachmentFlagAndConfigurationAndThrowIfNeeded(DocumentsOperationContext context, Attachment attachment, string documentId, string name)
        {
            if (attachment.Flags.HasFlag(AttachmentFlags.Retired))
            {
                throw new InvalidOperationException($"Cannot get attachment '{name}' on document '{documentId}' because it is retired. Please use dedicated API.");
            }

            return null;
        }

        protected virtual async Task WriteResponseStream(DocumentsOperationContext context, Attachment attachment, string collection, CancellationToken token)
        {
            await using (var stream = attachment.Stream)
            {
                await WriteAttachmentToResponseStream(context, stream, token);
            }
        }

        protected async Task WriteAttachmentToResponseStream(DocumentsOperationContext context, Stream stream, CancellationToken token)
        {
            using (context.GetMemoryBuffer(out var buffer))
            {
                var responseStream = RequestHandler.ResponseBodyStream();
                var count = stream.Read(buffer.Memory.Memory.Span); // can never wait, so no need for async
                while (count > 0)
                {
                    await responseStream.WriteAsync(buffer.Memory.Memory.Slice(0, count), token);
                    // we know that this can never wait, so no need to do async i/o here
                    count = stream.Read(buffer.Memory.Memory.Span);
                }
            }
        }
    }
}
