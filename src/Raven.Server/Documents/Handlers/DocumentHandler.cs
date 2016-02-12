// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;
using Sparrow;

namespace Raven.Server.Documents
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

            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
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
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();
                await GetDocumentsById(context, GetStringValuesQueryString("id"));
            }
        }

        [RavenAction("/databases/*/document", "POST", "/databases/{databaseName:string}/document body{documentsIds:string[]}")]
        public async Task PostGet()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var array = await context.ParseArrayToMemoryAsync(RequestBodyStream(), "queries",
                    BlittableJsonDocumentBuilder.UsageMode.None);

                var ids = new string[array.Count];
                for (int i = 0; i < array.Count; i++)
                {
                    ids[i] = array.GetStringByIndex(i);
                }

                context.Transaction = context.Environment.ReadTransaction();
                await GetDocumentsById(context, new StringValues(ids));
            }
        }

        private async Task GetDocumentsById(RavenOperationContext context, StringValues ids)
        {
            /* TODO: Call AddRequestTraceInfo
            AddRequestTraceInfo(sb =>
            {
                foreach (var id in ids)
                {
                    sb.Append("\t").Append(id).AppendLine();
                }
            });*/

            var documents = new Document[ids.Count];
            for (int i = 0; i < ids.Count; i++)
            {
                documents[i] = Database.DocumentsStorage.Get(context, ids[i]);
            }

            long actualEtag = ComputeEtagsFor(documents);
            if (GetLongFromHeaders("If-None-Match") == actualEtag)
            {
                HttpContext.Response.StatusCode = 304;
                return;
            }

            var includes = HttpContext.Request.Query["include"];
            var transformer = HttpContext.Request.Query["transformer"];
            if (includes.Count > 0)
            {
                //TODO: Transformer and includes
            }

            HttpContext.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
            HttpContext.Response.Headers["ETag"] = actualEtag.ToString();
            var writer = new BlittableJsonTextWriter(context, ResponseBodyStream());
            writer.WriteStartObject();
            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("Results"));
            WriteDocuments(context, writer, documents);
            writer.WriteComma();
            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("Includes"));
            writer.WriteStartArray();
            //TODO: Includes
            //TODO: Need to handle etags here as well
            writer.WriteEndArray();

            writer.WriteEndObject();
            writer.Flush();
        }

        private unsafe long ComputeEtagsFor(Document[] documents)
        {
            // This method is efficient because we aren't materializing any values
            // except the etag, which we need
            if (documents.Length == 1)
            {
                return documents[0]?.Etag ?? -1;
            }
            // we do this in a loop to avoid either large long array allocation on the heap
            // or busting the stack if we used stackalloc long[ids.Count]
            var ctx = Hashing.Streamed.XXHash64.BeginProcess();
            long* buffer = stackalloc long[4];//32 bytes
            Memory.Set((byte*)buffer, 0, sizeof(long) * 4);// not sure is stackalloc force init
            for (int i = 0; i < documents.Length; i += 4)
            {
                for (int j = 0; j < 4; j++)
                {
                    if (i + j >= documents.Length)
                        break;
                    var document = documents[i + j];
                    buffer[i] = document?.Etag ?? -1;
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
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var ids = HttpContext.Request.Query["id"];
                if (ids.Count == 0)
                    throw new ArgumentException("The 'id' query string parameter is mandatory");

                var id = ids[0];
                if (string.IsNullOrWhiteSpace(id))
                    throw new ArgumentException("The 'id' query string parameter must have a non empty value");

                var etag = GetLongFromHeaders("If-Match");

                context.Transaction = context.Environment.WriteTransaction();
                Database.DocumentsStorage.Delete(context, id, etag);
                context.Transaction.Commit();

                HttpContext.Response.StatusCode = 204; // NoContent

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/document", "PUT", "/databases/{databaseName:string}/document?id={documentId:string}")]
        public async Task Put()
        {
            RavenOperationContext context;
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
                using (context.Transaction = context.Environment.WriteTransaction())
                {
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

                var writer = new BlittableJsonTextWriter(context, ResponseBodyStream());
                context.Write(writer, reply);
                writer.Flush();
            }
        }

        [RavenAction("/databases/*/document", "PATCH", "/databases/{databaseName:string}/document?id={documentId:string}")]
        public async Task Patch()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                // TODO: We should implement here ScriptedPatchRequest as the EVAL function in v3.5. We retire the v3.0 PATCH method.
            }
        }
    }
}