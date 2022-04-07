using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Attachments
{
    internal class AttachmentHandlerProcessorForPutAttachment : AbstractAttachmentHandlerProcessorForPutAttachment<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public AttachmentHandlerProcessorForPutAttachment([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask<AttachmentDetails> PutAttachmentsAsync(DocumentsOperationContext context, string id, string name, Stream requestBodyStream, string contentType, string changeVector)
        {
            AttachmentDetails result;
            using (var streamsTempFile = RequestHandler.Database.DocumentsStorage.AttachmentsStorage.GetTempFile("put"))
            await using (var stream = streamsTempFile.StartNewStream())
            {
                string hash;
                try
                {
                    hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, requestBodyStream, stream, RequestHandler.Database.DatabaseShutdown);
                }
                catch (Exception)
                {
                    try
                    {
                        // if we failed to read the entire request body stream, we might leave
                        // data in the pipe still, this will cause us to read and discard the
                        // rest of the attachment stream and return the actual error to the caller
                        await requestBodyStream.CopyToAsync(Stream.Null);
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
                await stream.FlushAsync();
                await RequestHandler.Database.TxMerger.Enqueue(cmd);
                result = cmd.Result;
            }

            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

            return result;
        }
    }
}
