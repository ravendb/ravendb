// -----------------------------------------------------------------------
//  <copyright file="VersioningHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class VersioningHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/revisions", "GET", "/databases/{databaseName:string}/revisions?key={documentKey:string}&start={start:int|optional}&pageSize={pageSize:int|optional(25)")]
        public Task GetRevisionsFor()
        {
            var keys = GetQueryStringValueAndAssertIfSingleAndNotEmpty("key");
            var key = keys[0];

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                int start = GetIntValueQueryString("start", false) ?? 0;
                int take = GetIntValueQueryString("pageSize", false) ?? 25;
                var revisions = Database.DocumentsStorage.VersioningStorage.GetRevisions(context, key, start, take).ToList();

                long actualEtag = revisions.Count == 0 ? int.MinValue : revisions[revisions.Count - 1].Etag;
                if (GetLongFromHeaders("If-None-Match") == actualEtag)
                {
                    HttpContext.Response.StatusCode = 304;
                    return Task.CompletedTask;
                }

                HttpContext.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
                HttpContext.Response.Headers["ETag"] = actualEtag.ToString();
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteDocuments(context, revisions, false);
                }
            }

            return Task.CompletedTask;
        }
    }
}