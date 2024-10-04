using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal sealed class AttachmentHandlerProcessorForGetAttachment : AbstractAttachmentHandlerProcessorForGetAttachment<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForGetAttachment([NotNull] DatabaseRequestHandler requestHandler, bool isDocument) : base(requestHandler, isDocument)
        {
        }

        protected override async ValueTask GetAttachmentAsync(DocumentsOperationContext context, string documentId, string name, AttachmentType type, string changeVector, CancellationToken token)
        {
            using (context.OpenReadTransaction())
            {
                var attachment = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, documentId, name, type, changeVector);

                if (attachment == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

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

                var (sendBody, start, bytesRemaining) = RangeHelper.SetRangeHeaders(HttpContext, attachment.Size);
                if (!sendBody)
                    return;

                using (context.GetMemoryBuffer(out var buffer))
                await using (var stream = attachment.Stream)
                {
                    if (start > 0)
                    {
                        stream.Seek(start, SeekOrigin.Begin);
                    }

                    var responseStream = RequestHandler.ResponseBodyStream();
                    while (true)
                    {
                        if (bytesRemaining is <= 0)
                        {
                            return;
                        }

                        var readLength = buffer.Size;
                        if (bytesRemaining.HasValue)
                        {
                            readLength = (int)Math.Min(bytesRemaining.Value, readLength);
                        }

                        var read = stream.Read(buffer.Memory.Memory.Span.Slice(0, readLength)); // can never wait, so no need for async

                        if (bytesRemaining.HasValue)
                        {
                            bytesRemaining -= read;
                        }

                        // End of the source stream.
                        if (read == 0)
                        {
                            return;
                        }

                        await responseStream.WriteAsync(buffer.Memory.Memory.Slice(0, read), token);
                    }
                }
            }
        }
    }
}
