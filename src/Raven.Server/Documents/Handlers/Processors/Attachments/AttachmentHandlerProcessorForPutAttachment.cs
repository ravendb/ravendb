using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal class AttachmentHandlerProcessorForPutAttachment : AbstractAttachmentHandlerProcessorForPutAttachment<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForPutAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask PutAttachmentsAsync(DocumentsOperationContext context, string id, string name, Stream requestBodyStream, string contentType, string changeVector, CancellationToken token)
        {
            AttachmentDetails result;
            using (var streamsTempFile = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetTempFile("put"))
            await using (var stream = streamsTempFile.StartNewStream())
            {
                string hash;
                try
                {
                    hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, requestBodyStream, stream, token);
                }
                catch (Exception)
                {
                    try
                    {
                        // if we failed to read the entire request body stream, we might leave
                        // data in the pipe still, this will cause us to read and discard the
                        // rest of the attachment stream and return the actual error to the caller
                        await requestBodyStream.CopyToAsync(Stream.Null, token);
                    }
                    catch (Exception)
                    {
                        // we tried, but we can't clean the request, so let's just kill
                        // the connection
                        HttpContext.Abort();
                    }
                    throw;
                }

                var changeVectorLazy = context.GetLazyString(changeVector);

                var cmd = new AttachmentHandler.MergedPutAttachmentCommand
                {
                    Database = RequestHandler.Database,
                    ExpectedChangeVector = changeVectorLazy,
                    DocumentId = id,
                    Name = name,
                    Stream = stream,
                    Hash = hash,
                    ContentType = contentType
                };
                await stream.FlushAsync(token);
                await RequestHandler.Database.TxMerger.Enqueue(cmd);
                result = cmd.Result;
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(AttachmentDetails.ChangeVector));
                writer.WriteString(result.ChangeVector);
                writer.WriteComma();

                writer.WritePropertyName(nameof(AttachmentDetails.Name));
                writer.WriteString(result.Name);
                writer.WriteComma();

                writer.WritePropertyName(nameof(AttachmentDetails.DocumentId));
                writer.WriteString(result.DocumentId);
                writer.WriteComma();

                writer.WritePropertyName(nameof(AttachmentDetails.ContentType));
                writer.WriteString(result.ContentType);
                writer.WriteComma();

                writer.WritePropertyName(nameof(AttachmentDetails.Hash));
                writer.WriteString(result.Hash);
                writer.WriteComma();

                writer.WritePropertyName(nameof(AttachmentDetails.Size));
                writer.WriteInteger(result.Size);

                writer.WriteEndObject();
            }
        }
    }
}
