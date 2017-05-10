// -----------------------------------------------------------------------
//  <copyright file="VersioningHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Exceptions.Versioning;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class VersioningHandler : DatabaseRequestHandler
    {

        [RavenAction("/databases/*/versioning/config", "GET")]
        public Task GetVersioningConfig()
        {
            TransactionOperationContext context;
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = Server.ServerStore.Cluster.ReadDatabase(context, Database.Name);
                var versioningConfig = databaseRecord?.Versioning;
                if (versioningConfig != null)
                {

                    var versioningCollection = new DynamicJsonValue();
                    foreach (var collection in versioningConfig.Collections)
                    {
                        versioningCollection[collection.Key] = collection.Value.ToJson();
                    }

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(versioningConfig.Default)] = versioningConfig.Default.ToJson(),
                            [nameof(versioningConfig.Collections)] = versioningCollection
                        });
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }
            return Task.CompletedTask;
        }

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
                        writer.WriteDocuments(context, new[] { revision }, metadataOnly: false, numberOfResults: out int _);
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
                int pageSize = GetPageSize();
                var result = versioningStorage.GetRevisions(context, key, start, pageSize);
                var revisions = result.Revisions;

                long actualEtag = revisions.Length == 0 ? -1 : revisions[0].Etag;
                if (GetLongFromHeaders("If-None-Match") == actualEtag)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return Task.CompletedTask;
                }

                HttpContext.Response.Headers["ETag"] = actualEtag.ToString();

                int count;
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteDocuments(context, revisions, metadataOnly, out count);

                    writer.WriteComma();

                    writer.WritePropertyName("TotalResults");
                    writer.WriteInteger(result.Count);
                    writer.WriteEndObject();
                }

                AddPagingPerformanceHint(PagingOperationType.Revisions, nameof(GetRevisions), count, pageSize);
            }

            return Task.CompletedTask;
        }
    }
}