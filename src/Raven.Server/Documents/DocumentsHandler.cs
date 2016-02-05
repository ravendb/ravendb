// -----------------------------------------------------------------------
//  <copyright file="GetDocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.Routing;

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
            if(string.IsNullOrWhiteSpace(ids[0]))
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

        [RavenAction("/databases/*/docs","GET")]
        public async Task Get()
        {
            if (HttpContext.Request.Query.ContainsKey("id"))
            {
                await GetDocumentsById();
                return;
            }

            if (HttpContext.Request.Query.ContainsKey("etag"))
            {
                await GetDocumentsAfterEtag();
                return;
            }

            if (HttpContext.Request.Query.ContainsKey("startsWith"))
            {
                await GetDocumentsStartingWith();
                return;
            }
            await GetRecentDocuments();
        }

        private async Task GetDocumentsStartingWith()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();
                
                await WriteDocuments(context,
                    DocumentsStorage.GetDocumentsStartingWith(context,
                    HttpContext.Request.Query["startsWith"],
                    HttpContext.Request.Query["matches"],
                    HttpContext.Request.Query["excludes"],
                    GetStart(),
                    GetPageSize()
                    ));
            }
        }
        private async Task GetDocumentsAfterEtag()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();
                await WriteDocuments(context, DocumentsStorage.GetDocumentsAfter(context,
                    GetLongQueryString("etag"), GetStart(), GetPageSize()));
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

        private async Task GetRecentDocuments()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();

                await WriteDocuments(context,
                        DocumentsStorage.GetDocumentsInReverseEtagOrder(context, GetStart(), GetPageSize()));
            }
        }

        private async Task GetDocumentsById()
        {
            RavenOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.Transaction = context.Environment.ReadTransaction();
                //TODO: Etag handling
                var writer = new BlittableJsonTextWriter(context, HttpContext.Response.Body);
                writer.WriteStartObject();
                writer.WritePropertyName(context.GetLazyStringFor("Results"));
                writer.WriteStartArray();
                var first = true;
                foreach (var id in HttpContext.Request.Query["id"])
                {
                    var result = DocumentsStorage.Get(context, id);
                    if (result == null)
                        continue;
                    if (first == false)
                        writer.WriteComma();
                    first = false;
                    result.EnsureMetadata();

                    await context.WriteAsync(writer, result.Data);
                }
                writer.WriteEndArray();
                writer.WriteComma();
                writer.WritePropertyName(context.GetLazyStringFor("Includes"));
                writer.WriteStartArray();
                //TODO: Includes
                writer.WriteEndArray();

                writer.WriteEndObject();
                writer.Flush();
            }
        }
    }
}