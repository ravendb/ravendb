// -----------------------------------------------------------------------
//  <copyright file="VersioningHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Exceptions.Versioning;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class VersioningHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/revisions", "GET")]
        public Task GetRevisionsFor()
        {
            var versioningStorage = Database.BundleLoader.VersioningStorage;
            if (versioningStorage == null)
                throw new VersioningDisabledException();

            var etag = GetLongQueryString("etag", required: false);

            if (etag.HasValue)
            {
                return GetRevisionByEtag(etag.Value);
            }
            return GetRevisions();
        }

        private Task GetRevisionByEtag(long etag)
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var versioningStorage = Database.BundleLoader.VersioningStorage;
                var revision = versioningStorage.GetRevisionsFrom(context, etag, 1).FirstOrDefault();

                if (revision != null)
                {
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    { 
                        writer.WriteStartObject();
                        writer.WritePropertyName("Results");
                        writer.WriteDocuments(context, new [] { revision }, false);
                        writer.WriteEndObject();
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }

            return Task.CompletedTask;
        }

        private Task GetRevisions()
        {
            var key = GetQueryStringValueAndAssertIfSingleAndNotEmpty("key");
            var metadataOnly = GetBoolValueQueryString("metadata-only", required: false) ?? false;

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var versioningStorage = Database.BundleLoader.VersioningStorage;

                int start = GetStart();
                int take = GetPageSize(Database.Configuration.Core.MaxPageSize);
                var result = versioningStorage.GetRevisions(context, key, start, take);
                var revisions = result.revisions;

                long actualEtag = revisions.Length == 0 ? -1 : revisions[0].Etag;
                if (GetLongFromHeaders("If-None-Match") == actualEtag)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return Task.CompletedTask;
                }

                HttpContext.Response.Headers["ETag"] = actualEtag.ToString();

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteDocuments(context, revisions, metadataOnly);

                    writer.WriteComma();

                    writer.WritePropertyName("TotalResults");
                    writer.WriteInteger(result.count);
                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }
    }
}