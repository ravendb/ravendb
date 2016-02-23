// -----------------------------------------------------------------------
//  <copyright file="GetDocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/docs", "GET", "/databases/{databaseName:string}/docs")]
        public async Task GetDocuments()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

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
                    documents = Database.DocumentsStorage.GetDocumentsAfter(context,
                        GetLongQueryString("etag"), GetStart(), GetPageSize());
                }
                else if (HttpContext.Request.Query.ContainsKey("startsWith"))
                {
                    documents = Database.DocumentsStorage.GetDocumentsStartingWith(context,
                        HttpContext.Request.Query["startsWith"],
                        HttpContext.Request.Query["matches"],
                        HttpContext.Request.Query["excludes"],
                        GetStart(),
                        GetPageSize()
                        );
                }
                else // recent docs
                {
                    documents = Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, GetStart(), GetPageSize());
                }
                WriteDocuments(context, documents);
            }
        }

        private unsafe long ComputeAllDocumentsEtag(DocumentsOperationContext context)
        {
            var buffer = stackalloc long[2];

            buffer[0] = DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction);
            buffer[1] = Database.DocumentsStorage.GetNumberOfDocuments(context);

            return (long)Hashing.XXHash64.Calculate((byte*)buffer, sizeof(long) * 2);
        }
    }
}