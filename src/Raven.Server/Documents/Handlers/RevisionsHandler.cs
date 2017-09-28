// -----------------------------------------------------------------------
//  <copyright file="RevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Revisions;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;

namespace Raven.Server.Documents.Handlers
{
    public class RevisionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/revisions/config", "GET", AuthorizationStatus.ValidUser)]
        public Task GetRevisionsConfig()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = Server.ServerStore.Cluster.ReadDatabase(context, Database.Name);
                var revisionsConfig = databaseRecord?.Revisions;
                if (revisionsConfig != null)
                {
                    var revisionsCollection = new DynamicJsonValue();
                    foreach (var collection in revisionsConfig.Collections)
                    {
                        revisionsCollection[collection.Key] = collection.Value.ToJson();
                    }

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(revisionsConfig.Default)] = revisionsConfig.Default?.ToJson(),
                            [nameof(revisionsConfig.Collections)] = revisionsCollection
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

        [RavenAction("/databases/*/revisions", "GET", AuthorizationStatus.ValidUser)]
        public Task GetRevisionsFor()
        {
            var changeVector = GetStringQueryString("changeVector", required: false);
            if (changeVector != null)
            {
                return GetRevision(changeVector);
            }
            return GetRevisions();
        }

        private Task GetRevision(string changeVector)
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var revisionsStorage = Database.DocumentsStorage.RevisionsStorage;
                var revision = revisionsStorage.GetRevision(context, changeVector);
                if (revision == null)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return Task.CompletedTask;
                }

                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        writer.WriteStartObject();
                        writer.WritePropertyName("Results");
                        writer.WriteDocuments(context, new[] { revision }, metadataOnly: false, numberOfResults: out int _);
                        writer.WriteEndObject();
                    }
                }

            return Task.CompletedTask;
        }

        private Task GetRevisions()
        {
            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var metadataOnly = GetBoolValueQueryString("metadata-only", required: false) ?? false;

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var revisionsStorage = Database.DocumentsStorage.RevisionsStorage;

                var sw = Stopwatch.StartNew();
                var start = GetStart();
                var pageSize = GetPageSize();
                var revisions = revisionsStorage.GetRevisions(context, id, start, pageSize);

                var actualChangeVector = revisions.Revisions.Length == 0 ? "" : revisions.Revisions[0].ChangeVector;

                if (GetStringFromHeaders("If-None-Match") == actualChangeVector)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return Task.CompletedTask;
                }

                HttpContext.Response.Headers["ETag"] = "\"" + actualChangeVector + "\"";

                int count;
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    writer.WriteDocuments(context, revisions.Revisions, metadataOnly, out count);

                    writer.WriteComma();

                    writer.WritePropertyName("TotalResults");
                    writer.WriteInteger(revisions.Count);
                    writer.WriteEndObject();
                }

                AddPagingPerformanceHint(PagingOperationType.Revisions, nameof(GetRevisions), HttpContext, count, pageSize, sw.Elapsed);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/revisions/bin", "GET", AuthorizationStatus.ValidUser)]
        public Task GetRevisionsBin()
        {
            var revisionsStorage = Database.DocumentsStorage.RevisionsStorage;
            if (revisionsStorage.Configuration == null)
                throw new RevisionsDisabledException();

            var sw = Stopwatch.StartNew();
            var etag = GetLongQueryString("etag", false) ?? long.MaxValue;
            var pageSize = GetPageSize();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            using (revisionsStorage.GetLatestRevisionsBinEntryEtag(context, etag, out Slice revisionsBinEntryKey, out var actualChangeVector))
            {
                if (actualChangeVector != null)
                {
                    if (GetStringFromHeaders("If-None-Match") == actualChangeVector)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                        return Task.CompletedTask;
                    }

                    HttpContext.Response.Headers["ETag"] = "\"" + actualChangeVector + "\"";
                }

                int count;
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Results");
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, revisionsBinEntryKey, pageSize);
                    writer.WriteDocuments(context, revisions, false, out count);

                    writer.WriteEndObject();
                }

                AddPagingPerformanceHint(PagingOperationType.Revisions, nameof(GetRevisionsBin), HttpContext, count, pageSize, sw.Elapsed);
            }

            return Task.CompletedTask;
        }
    }
}
