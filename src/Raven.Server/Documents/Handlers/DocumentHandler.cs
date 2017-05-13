// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Exceptions.Transformers;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.Documents.Transformers;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using ConcurrencyException = Voron.Exceptions.ConcurrencyException;
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
            var pageSize = GetPageSize();
            var isStartsWith = HttpContext.Request.Query.ContainsKey("startsWith");

            IEnumerable<Document> documents;
            if (etag != null)
            {
                documents = Database.DocumentsStorage.GetDocumentsFrom(context, etag.Value, start, pageSize);
            }
            else if (isStartsWith)
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

            int numberOfResults;

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
                        writer.WriteDocuments(context, scope.Transform(documents).ToList(), metadataOnly, out numberOfResults);
                    }
                }
                else
                {
                    writer.WriteDocuments(context, documents, metadataOnly, out numberOfResults);
                }

                writer.WriteEndObject();
            }

            AddPagingPerformanceHint(PagingOperationType.Documents, isStartsWith ? nameof(DocumentsStorage.GetDocumentsStartingWith) : nameof(GetDocuments), numberOfResults, pageSize);
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

            int numberOfResults;
            var blittable = GetBoolValueQueryString("blittable", required: false) ?? false;
            if (blittable)
            {
                WriteDocumentsBlittable(context, documentsToWrite, includes, out numberOfResults);
            }
            else
            {
                WriteDocumentsJson(context, metadataOnly, documentsToWrite, includeDocs, includes, out numberOfResults);
            }

            AddPagingPerformanceHint(PagingOperationType.Documents, nameof(GetDocumentsById), numberOfResults, documents.Count);
        }

        private void WriteDocumentsJson(DocumentsOperationContext context, bool metadataOnly, IEnumerable<Document> documentsToWrite,
            IncludeDocumentsCommand includeDocs, List<Document> includes, out int numberOfResults)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");
                writer.WriteDocuments(context, documentsToWrite, metadataOnly, out numberOfResults);

                includeDocs.Fill(includes);

                writer.WriteComma();
                writer.WritePropertyName("Includes");
                if (includes.Count > 0)
                {
                    writer.WriteDocuments(context, includes, metadataOnly, out numberOfResults);
                }
                else
                {
                    writer.WriteStartArray();
                    writer.WriteEndArray();
                }

                writer.WriteEndObject();
            }
        }

        private void WriteDocumentsBlittable(DocumentsOperationContext context, IEnumerable<Document> documentsToWrite, List<Document> includes, out int numberOfResults)
        {
            numberOfResults = 0;
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
                    numberOfResults++;
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

                var cmd = new DeleteDocumentCommand(id, etag, Database, catchConcurrencyErrors: true);

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

                var cmd = new MergedPutCommand(doc, id, etag, Database);

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
        public async Task Patch()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

            var etag = GetLongFromHeaders("If-Match");
            var isTest = GetBoolValueQueryString("test", required: false) ?? false;
            var debugMode = GetBoolValueQueryString("debug", required: false) ?? isTest;
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

                BlittableJsonReaderObject origin = null;
                try
                {
                    var command = Database.Patcher.GetPatchDocumentCommand(id, etag, patch, patchIfMissing, skipPatchIfEtagMismatch, debugMode, isTest);

                    if (isTest == false)
                        await Database.TxMerger.Enqueue(command);
                    else
                    {
                        using (patch.IsPuttingDocuments == false ? 
                            context.OpenReadTransaction() : 
                            context.OpenWriteTransaction()) // PutDocument requires the write access to the docs storage
                        {
                            command.Execute(context);

                            // origin document is only accessible from the transaction, and we are closing it
                            // so we have hold on to a copy of it
                            origin = command.PatchResult.OriginalDocument.Clone(context);
                        }
                    }

                    switch (command.PatchResult.Status)
                    {
                        case PatchStatus.DocumentDoesNotExist:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            return;
                        case PatchStatus.Created:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                            break;
                        case PatchStatus.Skipped:
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                            return;
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

                        writer.WritePropertyName(nameof(command.PatchResult.Status));
                        writer.WriteString(command.PatchResult.Status.ToString());
                        writer.WriteComma();

                        writer.WritePropertyName(nameof(command.PatchResult.ModifiedDocument));
                        writer.WriteObject(command.PatchResult.ModifiedDocument);

                        if (debugMode)
                        {
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(command.PatchResult.OriginalDocument));
                            if (origin != null)
                                writer.WriteObject(origin);
                            else
                                writer.WriteNull();

                            writer.WriteComma();

                            writer.WritePropertyName(nameof(command.PatchResult.Debug));
                            if (command.PatchResult.Debug != null)
                                writer.WriteObject(command.PatchResult.Debug);
                            else
                                writer.WriteNull();
                        }

                        writer.WriteEndObject();
                    }
                }
                finally
                {
                    origin?.Dispose();
                }
            }
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
            private readonly string _id;
            private readonly long? _expectedEtag;
            private readonly BlittableJsonReaderObject _document;
            private readonly DocumentDatabase _database;

            public ExceptionDispatchInfo ExceptionDispatchInfo;
            public DocumentsStorage.PutOperationResults PutResult;

            public MergedPutCommand(BlittableJsonReaderObject doc, string id, long? etag, DocumentDatabase database)
            {
                _document = doc;
                _id = id;
                _expectedEtag = etag;
                _database = database;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                try
                {
                    PutResult = _database.DocumentsStorage.Put(context, _id, _expectedEtag, _document);
                }
                catch (ConcurrencyException e)
                {
                    ExceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
                }
                return 1;
            }
        }
    }
}