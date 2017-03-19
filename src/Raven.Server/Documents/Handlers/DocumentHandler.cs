// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Exceptions.Transformers;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Transformers;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron.Exceptions;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/docs", "HEAD", "/databases/{databaseName:string}/docs?id={documentId:string}")]
        public Task Head()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var etag = GetLongFromHeaders("If-None-Match");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, id);
                if (document == null)
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                else
                {
                    if (etag == document.Etag)
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    else
                        HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + document.Etag + "\"";
                }

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/docs", "GET", "/databases/{databaseName:string}/docs?id={documentId:string|multiple}&include={fieldName:string|optional|multiple}&transformer={transformerName:string|optional}")]
        public Task Get()
        {
            var ids = GetStringValuesQueryString("id", required: false);
            var metadataOnly = GetBoolValueQueryString("metadata-only", required: false) ?? false;
            var transformerName = GetStringQueryString("transformer", required: false);

            Transformer transformer = null;
            if (string.IsNullOrEmpty(transformerName) == false)
            {
                transformer = Database.TransformerStore.GetTransformer(transformerName);
                if (transformer == null)
                    TransformerDoesNotExistException.ThrowFor(transformerName);
            }

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                if (ids.Count > 0)
                    GetDocumentsById(context, ids, transformer, metadataOnly);
                else
                    GetDocuments(context, transformer, metadataOnly);

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/docs", "POST", "/databases/{databaseName:string}/docs body{documentsIds:string[]}")]
        public async Task PostGet()
        {
            var metadataOnly = GetBoolValueQueryString("metadata-only", required: false) ?? false;

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var parseArrayResults = await context.ParseArrayToMemoryAsync(RequestBodyStream(), "docs", BlittableJsonDocumentBuilder.UsageMode.None);

                var array = parseArrayResults.Item1;
                using (parseArrayResults.Item2)
                {
                    var ids = new string[array.Length];
                    for (int i = 0; i < array.Length; i++)
                    {
                        ids[i] = array.GetStringByIndex(i);
                    }

                    context.OpenReadTransaction();
                    GetDocumentsById(context, new StringValues(ids), null, metadataOnly);
                }
            }
        }

        [RavenAction("/databases/*/bulk_insert", "GET", "/databases/*/bulk_insert")]
        public async Task BulkInsert()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var socket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var list = new List<Document>();
                try
                {
                    var totalSize = 0;
                    while (true)
                    {
                        var task = context.ReadFromWebSocket(socket, "bulk-insert", Server.ServerStore.ServerShutdown);
                        if (task.IsCompleted == false || totalSize > 16*Voron.Global.Constants.Size.Megabyte)
                        {
                            await FlushBatchAsync(list);
                            totalSize = 0;
                        }

                        var obj = await task;
                        if (obj == null)
                            break;
                        totalSize += obj.Size;

                        LazyStringValue id;
                        var data = BuildObjectFromBulkInsertOp(context,obj,out id);
                        
                        var doc = new Document
                        {
                            Key = id,
                            Data = data
                        };
                        list.Add(doc);
                    }
                    await FlushBatchAsync(list);
                }
                catch (Exception e)
                {
                    using (var ms = new MemoryStream())
                    using (var writer = new BlittableJsonTextWriter(context, ms))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("Exception");
                        writer.WriteString(e.ToString());
                        writer.WriteEndObject();
                        writer.Flush();
                        ArraySegment<byte> bytes;
                        ms.TryGetBuffer(out bytes);
                        await socket.SendAsync(bytes, WebSocketMessageType.Text, true, Server.ServerStore.ServerShutdown);
                    }
                }
                finally
                {
                    await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Bulk-insert", Server.ServerStore.ServerShutdown);
                }
            }
        }

        private static BlittableJsonReaderObject BuildObjectFromBulkInsertOp(JsonOperationContext ctx, BlittableJsonReaderObject obj,out LazyStringValue id)
        {
            BlittableJsonReaderObject content;
            BlittableJsonReaderObject metadata;

            obj.TryGet(Constants.Documents.BulkInsert.Content, out content);
            obj.TryGet(Constants.Documents.Metadata.Key, out metadata);

            string collection;
            string clrType;

            metadata.TryGet(Constants.Documents.Metadata.Id, out id);
            metadata.TryGet(Constants.Documents.Metadata.Collection, out collection);
            metadata.TryGet(Constants.Documents.Metadata.RavenClrType, out clrType);
            content.Modifications = new DynamicJsonValue(content)
            {
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.Id] = id
                }
            };
            var modifiedMetadata = (DynamicJsonValue)content.Modifications[Constants.Documents.Metadata.Key];
            if (collection != null)
                modifiedMetadata[Constants.Documents.Metadata.Collection] = collection;
            if(clrType != null)
                modifiedMetadata[Constants.Documents.Metadata.RavenClrType] = clrType;

            return ctx.ReadObject(content,id);
        }

        private async Task FlushBatchAsync(List<Document> list)
        {
            if (list.Count == 0)
                return;
            
            await Database.TxMerger.Enqueue(new MergedInsertBulkCommand
            {
                Database = Database,
                Docs = list
            });
        }

        private class MergedInsertBulkCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public DocumentDatabase Database;
            public List<Document> Docs;

            public override void Execute(DocumentsOperationContext context)
            {
                foreach (var doc in Docs)
                {
                    Database.DocumentsStorage.Put(context, doc.Key, null, doc.Data);
                }
            }
        }

        private void GetDocuments(DocumentsOperationContext context, Transformer transformer, bool metadataOnly)
        {
            // everything here operates on all docs
            var actualEtag = DocumentsStorage.ComputeEtag(
                DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction), Database.DocumentsStorage.GetNumberOfDocuments(context)
            );

            if (GetLongFromHeaders("If-None-Match") == actualEtag)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }
            HttpContext.Response.Headers["ETag"] = "\"" + actualEtag + "\"";

            var etag = GetLongQueryString("etag", false);
            var start = GetStart();
            var pageSize = GetPageSize(Database.Configuration.Core.MaxPageSize);

            IEnumerable<Document> documents;
            if (etag != null)
            {
                documents = Database.DocumentsStorage.GetDocumentsFrom(context, etag.Value, start, pageSize);
            }
            else if (HttpContext.Request.Query.ContainsKey("startsWith"))
            {
                documents = Database.DocumentsStorage.GetDocumentsStartingWith(context,
                     HttpContext.Request.Query["startsWith"],
                     HttpContext.Request.Query["matches"],
                     HttpContext.Request.Query["exclude"],
                     HttpContext.Request.Query["startAfter"],
                     start,
                     pageSize);
            }
            else // recent docs
            {
                documents = Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, start, pageSize);
            }

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");

                if (transformer != null)
                {
                    using (var transformerParameters = GetTransformerParameters(context))
                    using (var scope = transformer.OpenTransformationScope(transformerParameters, null, Database.DocumentsStorage,
                        Database.TransformerStore, context))
                    {
                        writer.WriteDocuments(context, scope.Transform(documents).ToList(), metadataOnly);
                    }
                }
                else
                {
                    writer.WriteDocuments(context, documents, metadataOnly);
                }

                writer.WriteEndObject();
            }
        }

        private void GetDocumentsById(DocumentsOperationContext context, StringValues ids, Transformer transformer, bool metadataOnly)
        {
            var includePaths = GetStringValuesQueryString("include", required: false);
            var documents = new List<Document>(ids.Count);
            List<long> etags = null;
            var includes = new List<Document>(includePaths.Count * ids.Count);
            var includeDocs = new IncludeDocumentsCommand(Database.DocumentsStorage, context, includePaths);
            foreach (var id in ids)
            {
                var document = Database.DocumentsStorage.Get(context, id);
                if (ids.Count == 1 && document == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                documents.Add(document);
                includeDocs.Gather(document);
            }

            IEnumerable<Document> documentsToWrite;
            if (transformer != null)
            {
                var transformerParameters = GetTransformerParameters(context);

                using (var scope = transformer.OpenTransformationScope(transformerParameters, includeDocs, Database.DocumentsStorage, Database.TransformerStore, context))
                {
                    documentsToWrite = scope.Transform(documents).ToList();
                    etags = scope.LoadedDocumentEtags;
                }
            }
            else
                documentsToWrite = documents;

            includeDocs.Fill(includes);

            var actualEtag = ComputeEtagsFor(documents, includes, etags);
            if (transformer != null)
                actualEtag ^= transformer.Hash;

            var etag = GetLongFromHeaders("If-None-Match");
            if (etag == actualEtag)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + actualEtag + "\"";

            var blittable = GetBoolValueQueryString("blittable", required: false) ?? false;

            if (blittable)
            {
                WriteDocumentsBlittable(context, documentsToWrite, includes);
            }
            else
            {
                WriteDocumentsJson(context, metadataOnly, documentsToWrite, includeDocs, includes);
            }
        }

        private void WriteDocumentsJson(DocumentsOperationContext context, bool metadataOnly, IEnumerable<Document> documentsToWrite,
            IncludeDocumentsCommand includeDocs, List<Document> includes)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");
                writer.WriteDocuments(context, documentsToWrite, metadataOnly);

                includeDocs.Fill(includes);

                writer.WriteComma();
                writer.WritePropertyName("Includes");
                if (includes.Count > 0)
                {
                    writer.WriteDocuments(context, includes, metadataOnly);
                }
                else
                {
                    writer.WriteStartArray();
                    writer.WriteEndArray();
                }

                writer.WriteEndObject();
            }
        }

        private void WriteDocumentsBlittable(DocumentsOperationContext context, IEnumerable<Document> documentsToWrite, List<Document> includes)
        {
            HttpContext.Response.Headers["Content-Type"] = "binary/blittable-json";

            using (var streamBuffer = new UnmanagedStreamBuffer(context, ResponseBodyStream()))
            using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedStreamBuffer>(context,
                null, new BlittableWriter<UnmanagedStreamBuffer>(context, streamBuffer)))
            {
                writer.StartWriteObjectDocument();

                writer.StartWriteObject();
                writer.WritePropertyName("Results");

                writer.StartWriteArray();

                foreach (var document in documentsToWrite)
                {
                    writer.WriteEmbeddedBlittableDocument(document.Data);
                }

                writer.WriteArrayEnd();

                writer.WritePropertyName("Includes");

                writer.StartWriteArray();

                foreach (var include in includes)
                {
                    writer.WriteEmbeddedBlittableDocument(include.Data);
                }

                writer.WriteArrayEnd();

                writer.WriteObjectEnd();

                writer.FinalizeDocument();
            }
        }

        private static unsafe long ComputeEtagsFor(List<Document> documents, List<Document> includes, List<long> additionalEtags)
        {
            // This method is efficient because we aren't materializing any values
            // except the etag, which we need
            if (documents.Count == 1 && (includes == null || includes.Count == 0) && (additionalEtags == null || additionalEtags.Count == 0))
                return documents[0]?.Etag ?? -1;

            var documentsCount = documents.Count;
            var includesCount = includes?.Count ?? 0;
            var additionalEtagsCount = additionalEtags?.Count ?? 0;
            var count = documentsCount + includesCount + additionalEtagsCount;

            // we do this in a loop to avoid either large long array allocation on the heap
            // or busting the stack if we used stackalloc long[ids.Count]
            var ctx = Hashing.Streamed.XXHash64.BeginProcess();
            long* buffer = stackalloc long[4];//32 bytes
            Memory.Set((byte*)buffer, 0, sizeof(long) * 4);// not sure is stackalloc force init
            for (int i = 0; i < count; i += 4)
            {
                for (int j = 0; j < 4; j++)
                {
                    var index = i + j;
                    if (index >= count)
                        break;

                    if (index < documentsCount)
                    {
                        var document = documents[index];
                        buffer[j] = document?.Etag ?? -1;
                        continue;
                    }

                    if (includesCount > 0 && index >= documentsCount && index < documentsCount + includesCount)
                    {
                        var document = includes[index - documentsCount];
                        buffer[j] = document?.Etag ?? -1;
                        continue;
                    }

                    buffer[j] = additionalEtags[i + j - documentsCount - includesCount];
                }
                // we don't care if we didn't get to the end and have values from previous iteration
                // it will still be consistent, and that is what we care here.
                ctx = Hashing.Streamed.XXHash64.Process(ctx, (byte*)buffer, sizeof(long) * 4);
            }
            return (long)Hashing.Streamed.XXHash64.EndProcess(ctx);
        }

        [RavenAction("/databases/*/docs", "DELETE", "/databases/{databaseName:string}/docs?id={documentId:string}")]
        public async Task Delete()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

                var etag = GetLongFromHeaders("If-Match");

                var cmd = new MergedDeleteCommand
                {
                    Key = id,
                    Database = Database,
                    ExpectedEtag = etag
                };

                await Database.TxMerger.Enqueue(cmd);

                cmd.ExceptionDispatchInfo?.Throw();

                NoContentStatus();
            }
        }

        [RavenAction("/databases/*/docs", "PUT", "/databases/{databaseName:string}/docs?id={documentId:string}")]
        public async Task Put()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

                var doc = await context.ReadForDiskAsync(RequestBodyStream(), id);

                var etag = GetLongFromHeaders("If-Match");

                var cmd = new MergedPutCommand
                {
                    Database = Database,
                    ExpectedEtag = etag,
                    Key = id,
                    Document = doc
                };

                await Database.TxMerger.Enqueue(cmd);

                cmd.ExceptionDispatchInfo?.Throw();

                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(PutResult.Key));
                    writer.WriteString(cmd.PutResult.Key);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(PutResult.ETag));
                    writer.WriteInteger(cmd.PutResult.Etag);

                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/docs", "PATCH", "/databases/{databaseName:string}/docs?id={documentId:string}&test={isTestOnly:bool|optional(false)} body{ Patch:PatchRequest, PatchIfMissing:PatchRequest }")]
        public Task Patch()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

            var etag = GetLongFromHeaders("If-Match");
            var isTestOnly = GetBoolValueQueryString("test", required: false) ?? false;
            var isDebugOnly = GetBoolValueQueryString("debug", required: false) ?? isTestOnly;
            var skipPatchIfEtagMismatch = GetBoolValueQueryString("skipPatchIfEtagMismatch", required: false) ?? false;

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var request = context.Read(RequestBodyStream(), "ScriptedPatchRequest");

                BlittableJsonReaderObject patchCmd, patchIfMissingCmd;
                if (request.TryGet("Patch", out patchCmd) == false || patchCmd == null)
                    throw new ArgumentException("The 'Patch' field in the body request is mandatory");

                var patch = PatchRequest.Parse(patchCmd);

                PatchRequest patchIfMissing = null;
                if (request.TryGet("PatchIfMissing", out patchIfMissingCmd) && patchIfMissingCmd != null)
                    patchIfMissing = PatchRequest.Parse(patchIfMissingCmd);

                // TODO: In order to properly move this to the transaction merger, we need
                // TODO: move a lot of the costs (such as script parsing) out, so we create
                // TODO: an object that we'll apply, otherwise we'll slow down a lot the transactions
                // TODO: just by doing the javascript parsing and preparing the engine

                BlittableJsonReaderObject origin = null;
                try
                {
                    PatchResult patchResult;
                    using (context.OpenWriteTransaction())
                    {
                        patchResult = Database.Patch.Apply(context, id, etag, patch, patchIfMissing, skipPatchIfEtagMismatch, debugMode: isDebugOnly);

                        if (isTestOnly == false)
                        {
                            context.Transaction.Commit();
                        }
                        else
                        {
                            // origin document is only accessible from the transaction, and we are closing it
                            // so we have hold on to a copy of it
                            origin = patchResult.OriginalDocument.Clone(context);
                        }
                    }

                    switch (patchResult.Status)
                    {
                        case PatchStatus.DocumentDoesNotExist:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            return Task.CompletedTask;
                        case PatchStatus.Created:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                            break;
                        case PatchStatus.Skipped:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                            return Task.CompletedTask;
                        case PatchStatus.Patched:
                        case PatchStatus.NotModified:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();

                        writer.WritePropertyName(nameof(patchResult.Status));
                        writer.WriteString(patchResult.Status.ToString());
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(patchResult.ModifiedDocument));
                        writer.WriteObject(patchResult.ModifiedDocument);

                        if (isDebugOnly)
                        {
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(patchResult.OriginalDocument));
                            if (origin != null)
                                writer.WriteObject(origin);
                            else
                                writer.WriteNull();

                            writer.WritePropertyName(nameof(patchResult.Debug));
                            if (patchResult.Debug != null)
                                writer.WriteObject(patchResult.Debug);
                            else
                                writer.WriteNull();
                            writer.WriteComma();
                        }

                        writer.WriteEndObject();
                    }
                }
                finally
                {
                    origin?.Dispose();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/docs/class", "GET")]
        public Task GenerateClassFromDocument()
        {
            var id = GetStringQueryString("id");
            var lang = (GetStringQueryString("lang", required: false) ?? "csharp")
                .Trim().ToLowerInvariant();

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var document = Database.DocumentsStorage.Get(context, id);
                if (document == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return Task.CompletedTask;
                }

                switch (lang)
                {
                    case "csharp":
                        break;
                    default:
                        throw new NotImplementedException($"Document code generator isn't implemeted for {lang}");
                }

                using (var writer = new StreamWriter(ResponseBodyStream()))
                {
                    var codeGenerator = new JsonClassGenerator(lang);
                    var code = codeGenerator.Execute(document);
                    writer.Write(code);
                }

                return Task.CompletedTask;
            }
        }

        private class MergedPutCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public string Key;
            public long? ExpectedEtag;
            public BlittableJsonReaderObject Document;
            public DocumentDatabase Database;
            public ExceptionDispatchInfo ExceptionDispatchInfo;
            public DocumentsStorage.PutOperationResults PutResult;

            public override void Execute(DocumentsOperationContext context)
            {
                try
                {
                    PutResult = Database.DocumentsStorage.Put(context, Key, ExpectedEtag, Document);
                }
                catch (ConcurrencyException e)
                {
                    ExceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
                }
            }
        }

        private class MergedDeleteCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            public string Key;
            public long? ExpectedEtag;
            public DocumentDatabase Database;
            public ExceptionDispatchInfo ExceptionDispatchInfo;

            public override void Execute(DocumentsOperationContext context)
            {
                try
                {
                    Database.DocumentsStorage.Delete(context, Key, ExpectedEtag);
                }
                catch (ConcurrencyException e)
                {
                    ExceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
                }
            }
        }
    }
}