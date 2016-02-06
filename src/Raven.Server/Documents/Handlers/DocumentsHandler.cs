// -----------------------------------------------------------------------
//  <copyright file="GetDocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow;

namespace Raven.Server.Documents
{
    public class DocumentsHandler : DatabaseRequestHandler
    {

        [RavenAction("/databases/*/docs", "PUT")]
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

                var doc = await context.ReadForDisk(HttpContext.Request.Body, id);

                var etag = GetLongFromHeaders("If-Match");

                context.Transaction = context.Environment.WriteTransaction();
                if (id[id.Length - 1] == '/')
                {
                    id = id + DocumentsStorage.IdentityFor(context, id);
                }
                DocumentsStorage.Put(context, id, etag, doc);
                context.Transaction.Commit();

                HttpContext.Response.StatusCode = 201;
                HttpContext.Response.Headers["Location"] = id;
            }
        }


        [RavenAction("/databases/*/docs", "DELETE")]
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
                DocumentsStorage.Delete(context, id, etag);
                context.Transaction.Commit();

                HttpContext.Response.StatusCode = 204;

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/docs", "HEAD")]
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
                var document = DocumentsStorage.Get(context, ids[0]);
                if (document == null)
                    HttpContext.Response.StatusCode = 404;
                else
                    HttpContext.Response.Headers["ETag"] = document.Etag.ToString();
                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/queries", "POST")]
        public async Task QueriesPost()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var array = await context.ReadForMemory(RequestBodyStream(), "queries");

                await GetDocumentsById(context, HttpContext.Request.Query["id"]);
            }
        }

        [RavenAction("/databases/*/docs", "GET")]
        public async Task Get()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();
                if (HttpContext.Request.Query.ContainsKey("id"))
                {

                    await GetDocumentsById(context, HttpContext.Request.Query["id"]);
                    return;
                }


                // everything here operates on all docs
                var actualEtag = ComputeAllDocumentsEtag(context);

                if (GetLongFromHeaders("If-None-Match") == actualEtag)
                {
                    HttpContext.Response.StatusCode = 304;
                    return;
                }
                HttpContext.Response.Headers["ETag"] = actualEtag.ToString();

                IEnumerable<Document> documents;
                if (HttpContext.Request.Query.ContainsKey("etag"))
                {
                    documents = DocumentsStorage.GetDocumentsAfter(context,
                        GetLongQueryString("etag"), GetStart(), GetPageSize());
                }
                else if (HttpContext.Request.Query.ContainsKey("startsWith"))
                {
                    documents = DocumentsStorage.GetDocumentsStartingWith(context,
                        HttpContext.Request.Query["startsWith"],
                        HttpContext.Request.Query["matches"],
                        HttpContext.Request.Query["excludes"],
                        GetStart(),
                        GetPageSize()
                        );
                }
                else // recent docs
                {
                    documents = DocumentsStorage.GetDocumentsInReverseEtagOrder(context, GetStart(), GetPageSize());
                }
                await WriteDocuments(context, documents);
            }
        }

        private async Task WriteDocuments(RavenOperationContext context, IEnumerable<Document> documents)
        {
            var writer = new BlittableJsonTextWriter(context, HttpContext.Response.Body);
            writer.WriteStartArray();
            bool first = true;

            foreach (var document in documents)
            {
                if (first == false)
                    writer.WriteComma();
                first = false;

                document.EnsureMetadata();

                await context.WriteAsync(writer, document.Data);
            }

            writer.WriteEndArray();
            writer.Flush();
        }

        private unsafe long ComputeAllDocumentsEtag(RavenOperationContext context)
        {
            var buffer = stackalloc long[2];

            buffer[0] = DocumentsStorage.ReadLastEtag(context.Transaction);
            buffer[1] = DocumentsStorage.GetNumberOfDocuments(context);

            return (long)Hashing.XXHash64.Calculate((byte*)buffer, sizeof(long) * 2);
        }

        private async Task GetDocumentsById(RavenOperationContext context, StringValues ids)
        {
            var documents = new Document[ids.Count];
            for (int i = 0; i < ids.Count; i++)
            {
                documents[i] = DocumentsStorage.Get(context, ids[i]);
            }

            //TODO: Handle includes

            long actualEtag = ComputeEtagsFor(documents);

            if (GetLongFromHeaders("If-None-Match") == actualEtag)
            {
                HttpContext.Response.StatusCode = 304;
                return;
            }

            HttpContext.Response.Headers["ETag"] = actualEtag.ToString();
            var writer = new BlittableJsonTextWriter(context, HttpContext.Response.Body);
            writer.WriteStartObject();
            writer.WritePropertyName(context.GetLazyStringFor("Results"));
            writer.WriteStartArray();
            var first = true;
            foreach (var doc in documents)
            {
                if (doc == null)
                    continue;
                if (first == false)
                    writer.WriteComma();
                first = false;
                doc.EnsureMetadata();

                await context.WriteAsync(writer, doc.Data);
            }
            writer.WriteEndArray();
            writer.WriteComma();
            writer.WritePropertyName(context.GetLazyStringFor("Includes"));
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
    }
}