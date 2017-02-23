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
using Raven.Client.Documents.Operations;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron.Exceptions;

namespace Raven.Server.Documents.Handlers
{
    public class AttachmentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/attachments", "GET")]
        public async Task Get()
        {
            var documentId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var attachment = Database.DocumentsStorage.GetAttachment(context, documentId, name);
                if (attachment == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                var etag = GetLongFromHeaders("If-None-Match");
                if (etag == attachment.Etag)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                var fileName = Path.GetFileName(attachment.Name);
                HttpContext.Response.Headers["Content-Disposition"] = $"attachment; filename=\"{fileName}\"";
                HttpContext.Response.Headers["Content-Type"] = attachment.ContentType.ToString();
                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + attachment.Etag + "\"";

                JsonOperationContext.ManagedPinnedBuffer buffer;
                using (context.GetManagedBuffer(out buffer))
                using (var stream = attachment.Stream)
                {
                    var responseStream = ResponseBodyStream();
                    var count = await stream.ReadAsync(buffer.Buffer.Array, 0, buffer.Length, Database.DatabaseShutdown);
                    while (count > 0)
                    {
                        await responseStream.WriteAsync(buffer.Buffer.Array, 0, count, Database.DatabaseShutdown);
                        count = await stream.ReadAsync(buffer.Buffer.Array, 0, buffer.Length, Database.DatabaseShutdown);
                    }
                }
            }
        }

        [RavenAction("/databases/*/attachments", "PUT")]
        public async Task Put()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
                var contentType = GetStringQueryString("contentType", false) ?? "";

                AttachmentResult result;
                var tempPath = Path.Combine(Database.DocumentsStorage.Environment.Options.DataPager.Options.TempPath, $"attachment.{Guid.NewGuid():N}.put");
                using (var file = new FileStream(tempPath, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, 4096, FileOptions.DeleteOnClose | FileOptions.SequentialScan))
                {
                    JsonOperationContext.ManagedPinnedBuffer buffer;
                    using (context.GetManagedBuffer(out buffer))
                    {
                        var requestStream = RequestBodyStream();
                        var count = await requestStream.ReadAsync(buffer.Buffer.Array, 0, buffer.Length, Database.DatabaseShutdown);
                        while (count > 0)
                        {
                            await file.WriteAsync(buffer.Buffer.Array, 0, count, Database.DatabaseShutdown);
                            count = await requestStream.ReadAsync(buffer.Buffer.Array, 0, buffer.Length, Database.DatabaseShutdown);
                        }
                        file.Position = 0;

                        var etag = GetLongFromHeaders("If-Match");

                        var cmd = new MergedPutAttachmentCommand
                        {
                            Database = Database,
                            ExpectedEtag = etag,
                            DocumentId = id,
                            Name = name,
                            Stream = file,
                            ContentType = contentType,
                        };
                        await Database.TxMerger.Enqueue(cmd);
                        cmd.ExceptionDispatchInfo?.Throw();
                        result = cmd.Result;
                    }
                }
                // Linux does not clean the file, so we should clean it manually
                IOExtensions.DeleteFile(tempPath);

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(AttachmentResult.Etag));
                    writer.WriteInteger(result.Etag);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(AttachmentResult.Name));
                    writer.WriteString(result.Name);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(AttachmentResult.DocumentId));
                    writer.WriteString(result.DocumentId);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(AttachmentResult.ContentType));
                    writer.WriteString(result.ContentType);

                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/attachments", "DELETE")]
        public async Task Delete()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
                var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

                var etag = GetLongFromHeaders("If-Match");

                var cmd = new MergedDeleteAttachmentCommand
                {
                    Database = Database,
                    ExpectedEtag = etag,
                    DocumentId = id,
                    Name = name,
                };
                await Database.TxMerger.Enqueue(cmd);
                cmd.ExceptionDispatchInfo?.Throw();

                NoContentStatus();
            }
        }

        private class MergedPutAttachmentCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public string DocumentId;
            public string Name;
            public long? ExpectedEtag;
            public DocumentDatabase Database;
            public ExceptionDispatchInfo ExceptionDispatchInfo;
            public AttachmentResult Result;
            public string ContentType;
            public Stream Stream;

            public override void Execute(DocumentsOperationContext context)
            {
                try
                {
                    Result = Database.DocumentsStorage.PutAttachment(context, DocumentId, Name, ContentType, ExpectedEtag, Stream);
                }
                catch (ConcurrencyException e)
                {
                    ExceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
                }
            }
        }

        private class MergedDeleteAttachmentCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public string DocumentId;
            public string Name;
            public long? ExpectedEtag;
            public DocumentDatabase Database;
            public ExceptionDispatchInfo ExceptionDispatchInfo;

            public override void Execute(DocumentsOperationContext context)
            {
                try
                {
                    Database.DocumentsStorage.DeleteAttachment(context, DocumentId, Name, ExpectedEtag);
                }
                catch (ConcurrencyException e)
                {
                    ExceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
                }
            }
        }
    }
}