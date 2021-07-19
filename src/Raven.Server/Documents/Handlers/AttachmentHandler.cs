// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Exceptions;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Voron;

namespace Raven.Server.Documents.Handlers
{
    public class AttachmentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/attachments", "HEAD", AuthorizationStatus.ValidUser)]
        public Task Head()
        {
            var documentId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var attachment = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, documentId, name, AttachmentType.Document, null);
                if (attachment == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return Task.CompletedTask;
                }

                var changeVector = GetStringFromHeaders("If-None-Match");
                if (changeVector == attachment.ChangeVector)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return Task.CompletedTask;
                }

                HttpContext.Response.Headers[Constants.Headers.Etag] = $"\"{attachment.ChangeVector}\"";

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/attachments", "GET", AuthorizationStatus.ValidUser)]
        public Task Get()
        {
            return GetAttachment(true);
        }

        [RavenAction("/databases/*/attachments", "POST", AuthorizationStatus.ValidUser)]
        public Task GetPost()
        {
            return GetAttachment(false);
        }

        [RavenAction("/databases/*/debug/attachments/hash", "GET", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public Task Exists()
        {
            var hash = GetStringQueryString("hash");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            using (Slice.From(context.Allocator, hash, out var hashSlice))
            {
                var count = AttachmentsStorage.GetCountOfAttachmentsForHash(context, hashSlice);
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Hash");
                    writer.WriteString(hash);
                    writer.WriteComma();
                    writer.WritePropertyName("Count");
                    writer.WriteInteger(count);
                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/debug/attachments/metadata", "GET", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public Task GetDocumentsAttachmentMetadataWithCounts()
        {
            var id = GetStringQueryString("id", false);
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var array = Database.DocumentsStorage.AttachmentsStorage.GetAttachmentsMetadataForDocumentWithCounts(context, id.ToLowerInvariant());
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Id");
                    writer.WriteString(id);
                    writer.WriteComma();
                    writer.WriteArray("Attachments", array, context);
                    writer.WriteEndObject();
                }
            }
            return Task.CompletedTask;
        }

        private async Task GetAttachment(bool isDocument)
        {
            var documentId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var type = AttachmentType.Document;
                string changeVector = null;
                if (isDocument == false)
                {
                    var stream = TryGetRequestFromStream("ChangeVectorAndType") ?? RequestBodyStream();
                    var request = context.Read(stream, "GetAttachment");

                    if (request.TryGet("Type", out string typeString) == false ||
                        Enum.TryParse(typeString, out type) == false)
                        throw new ArgumentException("The 'Type' field in the body request is mandatory");

                    if (request.TryGet("ChangeVector", out changeVector) == false && changeVector != null)
                        throw new ArgumentException("The 'ChangeVector' field in the body request is mandatory");
                }

                var attachment = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, documentId, name, type, changeVector);
                if (attachment == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var attachmentChangeVector = GetStringFromHeaders("If-None-Match");
                if (attachmentChangeVector == attachment.ChangeVector)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                try
                {
                    var fileName = Path.GetFileName(attachment.Name);
                    fileName = Uri.EscapeDataString(fileName);
                    HttpContext.Response.Headers["Content-Disposition"] = $"attachment; filename=\"{fileName}\"; filename*=UTF-8''{fileName}";
                }
                catch (ArgumentException e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Skip Content-Disposition header because of not valid file name: {attachment.Name}", e);
                }
                try
                {
                    HttpContext.Response.Headers["Content-Type"] = attachment.ContentType.ToString();
                }
                catch (InvalidOperationException e)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info($"Skip Content-Type header because of not valid content type: {attachment.ContentType}", e);
                    if (HttpContext.Response.Headers.ContainsKey("Content-Type"))
                        HttpContext.Response.Headers.Remove("Content-Type");
                }
                HttpContext.Response.Headers["Attachment-Hash"] = attachment.Base64Hash.ToString();
                HttpContext.Response.Headers["Attachment-Size"] = attachment.Stream.Length.ToString();
                HttpContext.Response.Headers[Constants.Headers.Etag] = $"\"{attachment.ChangeVector}\"";

                using (context.GetManagedBuffer(out JsonOperationContext.ManagedPinnedBuffer buffer))
                using (var stream = attachment.Stream)
                {
                    var responseStream = ResponseBodyStream();
                    var count = stream.Read(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Length); // can never wait, so no need for async
                    while (count > 0)
                    {
                        await responseStream.WriteAsync(buffer.Buffer.Array, buffer.Buffer.Offset, count, Database.DatabaseShutdown);
                        // we know that this can never wait, so no need to do async i/o here
                        count = stream.Read(buffer.Buffer.Array, buffer.Buffer.Offset, buffer.Length);
                    }
                }
            }
        }

        [RavenAction("/databases/*/attachments", "PUT", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task Put()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
                var contentType = GetStringQueryString("contentType", false) ?? "";

                AttachmentDetails result;
                using (var streamsTempFile = Database.DocumentsStorage.AttachmentsStorage.GetTempFile("put"))
                using (var stream = streamsTempFile.StartNewStream())
                {
                    Stream requestBodyStream = RequestBodyStream();
                    string hash;
                    try
                    {
                        hash = await AttachmentsStorageHelper.CopyStreamToFileAndCalculateHash(context, requestBodyStream, stream, Database.DatabaseShutdown);
                    }
                    catch (Exception)
                    {
                        try
                        {
                            // if we failed to read the entire request body stream, we might leave
                            // data in the pipe still, this will cause us to read and discard the 
                            // rest of the attachment stream and return the actual error to the caller
                            requestBodyStream.CopyTo(Stream.Null);
                        }
                        catch (Exception)
                        {
                            // we tried, but we can't clean the request, so let's just kill
                            // the connection
                            HttpContext.Abort();
                        }
                        throw;
                    }

                    var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                    var cmd = new MergedPutAttachmentCommand
                    {
                        Database = Database,
                        ExpectedChangeVector = changeVector,
                        DocumentId = id,
                        Name = name,
                        Stream = stream,
                        Hash = hash,
                        ContentType = contentType
                    };
                    stream.Flush();
                    await Database.TxMerger.Enqueue(cmd);
                    result = cmd.Result;
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
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

        [RavenAction("/databases/*/attachments", "DELETE", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task Delete()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

                var changeVector = context.GetLazyString(GetStringFromHeaders("If-Match"));

                var cmd = new MergedDeleteAttachmentCommand
                {
                    Database = Database,
                    ExpectedChangeVector = changeVector,
                    DocumentId = id,
                    Name = name
                };
                await Database.TxMerger.Enqueue(cmd);

                NoContentStatus();
            }
        }

        public class MergedPutAttachmentCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public string DocumentId;
            public string Name;
            public LazyStringValue ExpectedChangeVector;
            public DocumentDatabase Database;
            public AttachmentDetails Result;
            public string ContentType;
            public Stream Stream;
            public string Hash;

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                Result = Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, DocumentId, Name, ContentType, Hash, ExpectedChangeVector, Stream);
                return 1;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new MergedPutAttachmentCommandDto
                {
                    DocumentId = DocumentId,
                    Name = Name,
                    ExpectedChangeVector = ExpectedChangeVector,
                    ContentType = ContentType,
                    Stream = Stream,
                    Hash = Hash
                };
            }
        }

        internal class MergedDeleteAttachmentCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public string DocumentId;
            public string Name;
            public LazyStringValue ExpectedChangeVector;
            public DocumentDatabase Database;

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                Database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, DocumentId, Name, ExpectedChangeVector);
                return 1;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new MergedDeleteAttachmentCommandDto
                {
                    DocumentId = DocumentId,
                    Name = Name,
                    ExpectedChangeVector = ExpectedChangeVector
                };
            }
        }
    }

    public class MergedPutAttachmentCommandDto : TransactionOperationsMerger.IReplayableCommandDto<AttachmentHandler.MergedPutAttachmentCommand>
    {
        public string DocumentId;
        public string Name;
        public LazyStringValue ExpectedChangeVector;
        public string ContentType;
        public Stream Stream;
        public string Hash;

        public AttachmentHandler.MergedPutAttachmentCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new AttachmentHandler.MergedPutAttachmentCommand
            {
                DocumentId = DocumentId,
                Name = Name,
                ExpectedChangeVector = ExpectedChangeVector,
                ContentType = ContentType,
                Stream = Stream,
                Hash = Hash,
                Database = database
            };
        }
    }

    internal class MergedDeleteAttachmentCommandDto : TransactionOperationsMerger.IReplayableCommandDto<AttachmentHandler.MergedDeleteAttachmentCommand>
    {
        public string DocumentId;
        public string Name;
        public LazyStringValue ExpectedChangeVector;

        public AttachmentHandler.MergedDeleteAttachmentCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            return new AttachmentHandler.MergedDeleteAttachmentCommand
            {
                DocumentId = DocumentId,
                Name = Name,
                ExpectedChangeVector = ExpectedChangeVector,
                Database = database
            };
        }
    }
}
