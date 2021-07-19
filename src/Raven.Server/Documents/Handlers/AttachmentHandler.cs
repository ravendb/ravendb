// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
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

        [RavenAction("/databases/*/attachments/bulk", "POST", AuthorizationStatus.ValidUser)]
        public async Task GetAttachments()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var request = await context.ReadForDiskAsync(RequestBodyStream(), "GetAttachments");

                if (request.TryGet(nameof(AttachmentType), out string typeString) == false || Enum.TryParse(typeString, out AttachmentType type) == false)
                    throw new ArgumentException($"The '{nameof(AttachmentType)}' field in the body request is mandatory");

                if (request.TryGet(nameof(GetAttachmentsOperation.GetAttachmentsCommand.Attachments), out BlittableJsonReaderArray attachments) == false)
                    throw new ArgumentException($"The '{nameof(GetAttachmentsOperation.GetAttachmentsCommand.Attachments)}' field in the body request is mandatory");

                var attachmentsStreams = new List<Stream>();
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(GetAttachmentsOperation.GetAttachmentsCommand.AttachmentsMetadata));
                    writer.WriteStartArray();
                    var first = true;
                    foreach (BlittableJsonReaderObject bjro in attachments)
                    {
                        if (bjro.TryGet(nameof(AttachmentRequest.DocumentId), out string id) == false)
                            throw new ArgumentException($"Could not parse {nameof(AttachmentRequest.DocumentId)}");
                        if (bjro.TryGet(nameof(AttachmentRequest.Name), out string name) == false)
                            throw new ArgumentException($"Could not parse {nameof(AttachmentRequest.Name)}");

                        var attachment = Database.DocumentsStorage.AttachmentsStorage.GetAttachment(context, id, name, type, changeVector: null);
                        if (attachment == null)
                            continue;

                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        attachment.Size = attachment.Stream.Length;
                        attachmentsStreams.Add(attachment.Stream);
                        WriteAttachmentDetails(writer, attachment, id);
                    }

                    writer.WriteEndArray();
                    writer.WriteEndObject();
                }

                using (context.GetMemoryBuffer(out var buffer))
                {
                    var responseStream = ResponseBodyStream();
                    foreach (var stream in attachmentsStreams)
                    {
                        await using (var tmpStream = stream)
                        {
                            var count = await tmpStream.ReadAsync(buffer.Memory.Memory);
                            while (count > 0)
                            {
                                await responseStream.WriteAsync(buffer.Memory.Memory.Slice(0, count), Database.DatabaseShutdown);
                                count = await tmpStream.ReadAsync(buffer.Memory.Memory);
                            }
                        }
                    }
                }
            }
        }

        private static void WriteAttachmentDetails(AsyncBlittableJsonTextWriter writer, Attachment attachment, string documentId)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(nameof(AttachmentDetails.Name));
            writer.WriteString(attachment.Name);
            writer.WriteComma();
            writer.WritePropertyName(nameof(AttachmentDetails.Hash));
            writer.WriteString(attachment.Base64Hash.ToString());
            writer.WriteComma();
            writer.WritePropertyName(nameof(AttachmentDetails.ContentType));
            writer.WriteString(attachment.ContentType);
            writer.WriteComma();
            writer.WritePropertyName(nameof(AttachmentDetails.Size));
            writer.WriteInteger(attachment.Size);
            writer.WriteComma();
            writer.WritePropertyName(nameof(AttachmentDetails.ChangeVector));
            writer.WriteString(attachment.ChangeVector);
            writer.WriteComma();
            writer.WritePropertyName(nameof(AttachmentDetails.DocumentId));
            writer.WriteString(documentId);
            writer.WriteEndObject();
        }

        [RavenAction("/databases/*/debug/attachments/hash", "GET", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task Exists()
        {
            var hash = GetStringQueryString("hash");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            using (Slice.From(context.Allocator, hash, out var hashSlice))
            {
                var count = AttachmentsStorage.GetCountOfAttachmentsForHash(context, hashSlice);
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
        }

        [RavenAction("/databases/*/debug/attachments/metadata", "GET", AuthorizationStatus.ValidUser, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetDocumentsAttachmentMetadataWithCounts()
        {
            var id = GetStringQueryString("id", false);
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var array = Database.DocumentsStorage.AttachmentsStorage.GetAttachmentsMetadataForDocumentWithCounts(context, id.ToLowerInvariant());
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Id");
                    writer.WriteString(id);
                    writer.WriteComma();
                    writer.WriteArray("Attachments", array, context);
                    writer.WriteEndObject();
                }
            }
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
                    var request = await context.ReadForDiskAsync(stream, "GetAttachment");

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

                using (context.GetMemoryBuffer(out var buffer))
                await using (var stream = attachment.Stream)
                {
                    var responseStream = ResponseBodyStream();
                    var count = stream.Read(buffer.Memory.Memory.Span); // can never wait, so no need for async
                    while (count > 0)
                    {
                        await responseStream.WriteAsync(buffer.Memory.Memory.Slice(0, count), Database.DatabaseShutdown);
                        // we know that this can never wait, so no need to do async i/o here
                        count = stream.Read(buffer.Memory.Memory.Span);
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
                await using (var stream = streamsTempFile.StartNewStream())
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
                    await stream.FlushAsync();
                    await Database.TxMerger.Enqueue(cmd);
                    result = cmd.Result;
                }

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
