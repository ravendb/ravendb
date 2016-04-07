// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Abstractions.Data;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/document", "HEAD", "/databases/{databaseName:string}/document?id={documentId:string}")]
        public Task Head()
        {
            var ids = HttpContext.Request.Query["id"];
            if (ids.Count != 1)
                throw new ArgumentException("Query string value 'id' must appear exactly once");
            if (string.IsNullOrWhiteSpace(ids[0]))
                throw new ArgumentException("Query string value 'id' must have a non empty value");

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var document = Database.DocumentsStorage.Get(context, ids[0]);
                if (document == null)
                    HttpContext.Response.StatusCode = 404;
                else
                    HttpContext.Response.Headers["ETag"] = document.Etag.ToString();
                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/document", "GET", "/databases/{databaseName:string}/document?id={documentId:string|multiple}&include={fieldName:string|optional|multiple}&transformer={transformerName:string|optional}")]
        public async Task Get()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                await GetDocumentsById(context, GetStringValuesQueryString("id"));
            }
        }

        [RavenAction("/databases/*/document", "POST", "/databases/{databaseName:string}/document body{documentsIds:string[]}")]
        public async Task PostGet()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var array = await context.ParseArrayToMemoryAsync(RequestBodyStream(), "queries",
                    BlittableJsonDocumentBuilder.UsageMode.None);

                var ids = new string[array.Length];
                for (int i = 0; i < array.Length; i++)
                {
                    ids[i] = array.GetStringByIndex(i);
                }

                context.OpenReadTransaction();
                await GetDocumentsById(context, new StringValues(ids));
            }
        }

        private Task GetDocumentsById(DocumentsOperationContext context, StringValues ids)
        {
            /* TODO: Call AddRequestTraceInfo
            AddRequestTraceInfo(sb =>
            {
                foreach (var id in ids)
                {
                    sb.Append("\t").Append(id).AppendLine();
                }
            });*/
            var includes = HttpContext.Request.Query["include"];
            var documents = new List<Document>(ids.Count + (includes.Count * ids.Count));
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (string id in ids)
            {
                documents.Add(Database.DocumentsStorage.Get(context, id));
            }

            var includeDocs = new IncludeDocumentsCommand(Database.DocumentsStorage, context, includes);
            includeDocs.Execute(documents, documents);

            long actualEtag = ComputeEtagsFor(documents);
            if (GetLongFromHeaders("If-None-Match") == actualEtag)
            {
                HttpContext.Response.StatusCode = 304;
                return Task.CompletedTask;
            }

            //TODO: Transformers
            //var transformer = HttpContext.Request.Query["transformer"];

            HttpContext.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
            HttpContext.Response.Headers["ETag"] = actualEtag.ToString();
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("Results"));

                WriteDocuments(context, writer, documents, 0, ids.Count);

                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("Includes"));

                WriteDocuments(context, writer, documents, ids.Count, documents.Count - ids.Count);

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }

        private unsafe long ComputeEtagsFor(List<Document> documents)
        {
            // This method is efficient because we aren't materializing any values
            // except the etag, which we need
            if (documents.Count == 1)
            {
                return documents[0]?.Etag ?? -1;
            }
            // we do this in a loop to avoid either large long array allocation on the heap
            // or busting the stack if we used stackalloc long[ids.Count]
            var ctx = Hashing.Streamed.XXHash64.BeginProcess();
            long* buffer = stackalloc long[4];//32 bytes
            Memory.Set((byte*)buffer, 0, sizeof(long) * 4);// not sure is stackalloc force init
            for (int i = 0; i < documents.Count; i += 4)
            {
                for (int j = 0; j < 4; j++)
                {
                    if (i + j >= documents.Count)
                        break;
                    var document = documents[i + j];
                    buffer[j] = document?.Etag ?? -1;
                }
                // we don't care if we didn't get to the end and have values from previous iteration
                // it will still be consistent, and that is what we care here.
                ctx = Hashing.Streamed.XXHash64.Process(ctx, (byte*)buffer, sizeof(long) * 4);
            }
            return (long)Hashing.Streamed.XXHash64.EndProcess(ctx);
        }

        [RavenAction("/databases/*/document", "DELETE", "/databases/{databaseName:string}/document?id={documentId:string}")]
        public Task Delete()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var ids = HttpContext.Request.Query["id"];
                if (ids.Count == 0)
                    throw new ArgumentException("The 'id' query string parameter is mandatory");

                var id = ids[0];
                if (string.IsNullOrWhiteSpace(id))
                    throw new ArgumentException("The 'id' query string parameter must have a non empty value");

                var etag = GetLongFromHeaders("If-Match");

                context.OpenWriteTransaction();
                Database.DocumentsStorage.Delete(context, id, etag);
                context.Transaction.Commit();

                HttpContext.Response.StatusCode = 204; // NoContent

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/document", "PUT", "/databases/{databaseName:string}/document?id={documentId:string}")]
        public async Task Put()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var ids = HttpContext.Request.Query["id"];
                if (ids.Count == 0)
                    throw new ArgumentException("The 'id' query string parameter is mandatory");

                var id = ids[0];
                if (string.IsNullOrWhiteSpace(id))
                    throw new ArgumentException("The 'id' query string parameter must have a non empty value");

                var doc = await context.ReadForDiskAsync(RequestBodyStream(), id);

                var etag = GetLongFromHeaders("If-Match");

                PutResult putResult;
                using (context.OpenWriteTransaction())
                {
                    Database.Metrics.DocPutsPerSecond.Mark();
                    putResult = Database.DocumentsStorage.Put(context, id, etag, doc);
                    context.Transaction.Commit();
                    // we want to release the transaction before we write to the network
                }

                HttpContext.Response.StatusCode = 201;

                var reply = new DynamicJsonValue
                {
                    ["Key"] = putResult.Key,
                    ["Etag"] = putResult.ETag
                };

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, reply);
                }
            }
        }

        [RavenAction("/databases/*/document", "PATCH", "/databases/{databaseName:string}/document?id={documentId:string}&test={isTestOnly:bool|optional(false)} body{ Patch:PatchRequest, PatchIfMissing:PatchRequest }")]
        public Task Patch()
        {
            var ids = HttpContext.Request.Query["id"];
            if (ids.Count == 0)
                throw new ArgumentException("The 'id' query string parameter is mandatory");

            var documentId = ids[0];
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("The 'id' query string parameter must have a non empty value");

            var etag = GetLongFromHeaders("If-Match");
            var isTestOnly = GetBoolValueQueryString("test", false);

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var request = context.Read(RequestBodyStream(), "ScriptedPatchRequest");

                BlittableJsonReaderObject patchCmd, patchIsMissingCmd;
                if (request.TryGet("Patch", out patchCmd) == false)
                    throw new ArgumentException("The 'Patch' field in the body request is mandatory");
                var patch = PatchRequest.Parse(patchCmd);

                PatchRequest patchIfMissing = null;
                if (request.TryGet("PatchIfMissing", out patchIsMissingCmd))
                {
                    patchIfMissing = PatchRequest.Parse(patchCmd);
                }

                PatchResultData patchResult;
                using (context.OpenWriteTransaction())
                {
                    patchResult = Database.Patch.Apply(context, documentId, etag, patch, patchIfMissing, isTestOnly);
                    context.Transaction.Commit();
                }

                Debug.Assert((patchResult.PatchResult == PatchResult.Patched) == (isTestOnly == false));
                var result = new DynamicJsonValue
                {
                    ["Patched"] = isTestOnly == false,
                    ["Debug"] = patchResult.ModifiedDocument,
                };
                if (isTestOnly)
                {
                    result["Document"] = patchResult.OriginalDocument;
                }
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }


            return Task.CompletedTask;
        }
    }
}