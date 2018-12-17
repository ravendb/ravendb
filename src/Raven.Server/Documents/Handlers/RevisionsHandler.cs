// -----------------------------------------------------------------------
//  <copyright file="RevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions.Documents.Revisions;
using Raven.Server.Documents.Revisions;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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

        [RavenAction("/databases/*/admin/revisions/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigRevisions()
        {
            await DatabaseConfigurations(ServerStore.ModifyDatabaseRevisions, "read-revisions-config");
        }

        [RavenAction("/databases/*/revisions", "GET", AuthorizationStatus.ValidUser)]
        public Task GetRevisionsFor()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var changeVectors = GetStringValuesQueryString("changeVector", required: false);
                var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;

                if (changeVectors.Count > 0)
                    GetRevisionByChangeVector(context, changeVectors, metadataOnly);
                else
                    GetRevisions(context, metadataOnly);

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/revisions/revert", "GET", AuthorizationStatus.ValidUser)]
        public async Task Revert()
        {
            var operationId = GetLongQueryString("operationId");
            var time = GetDateTimeQueryString("time");
            var window = GetTimeSpanQueryString("window", required: false) ?? TimeSpan.FromHours(48);

            var token = CreateOperationToken();

            await Database.Operations.AddOperation(
                Database,
                $"Revert database '{Database.Name}' to {time}.",
                Operations.Operations.OperationType.DatabaseRevert,
                onProgress => Task.Run(async () => await Database.DocumentsStorage.RevisionsStorage.RevertRevisions(time.Value, window, onProgress), token.Token),
                operationId,
                token: token);
        }

        private void GetRevisionByChangeVector(DocumentsOperationContext context, StringValues changeVectors, bool metadataOnly)
        {
            var revisionsStorage = Database.DocumentsStorage.RevisionsStorage;
            var sw = Stopwatch.StartNew();

            var revisions = new List<Document>(changeVectors.Count);
            foreach (var changeVector in changeVectors)
            {
                var revision = revisionsStorage.GetRevision(context, changeVector);
                if (revision == null && changeVectors.Count == 1)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    return;
                }

                revisions.Add(revision);
            }

            var actualEtag = ComputeHttpEtags.ComputeEtagForRevisions(revisions);

            var etag = GetStringFromHeaders("If-None-Match");
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
                WriteRevisionsBlittable(context, revisions, out numberOfResults);
            }
            else
            {
                WriteRevisionsJson(context, metadataOnly, revisions, out numberOfResults);
            }

            AddPagingPerformanceHint(PagingOperationType.Documents, nameof(GetRevisionByChangeVector), HttpContext.Request.QueryString.Value, numberOfResults, revisions.Count, sw.ElapsedMilliseconds);
        }

        private void WriteRevisionsJson(JsonOperationContext context, bool metadataOnly, IEnumerable<Document> documentsToWrite, out int numberOfResults)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(GetDocumentsResult.Results));
                writer.WriteDocuments(context, documentsToWrite, metadataOnly, out numberOfResults);
                writer.WriteEndObject();
            }
        }

        private void WriteRevisionsBlittable(DocumentsOperationContext context, IEnumerable<Document> documentsToWrite, out int numberOfResults)
        {
            numberOfResults = 0;
            HttpContext.Response.Headers["Content-Type"] = "binary/blittable-json";

            using (var streamBuffer = new UnmanagedStreamBuffer(context, ResponseBodyStream()))
            using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedStreamBuffer>(context,
                null, new BlittableWriter<UnmanagedStreamBuffer>(context, streamBuffer)))
            {
                writer.StartWriteObjectDocument();

                writer.StartWriteObject();
                writer.WritePropertyName(nameof(GetDocumentsResult.Results));

                writer.StartWriteArray();
                foreach (var document in documentsToWrite)
                {
                    numberOfResults++;
                    writer.WriteEmbeddedBlittableDocument(document.Data);
                }
                writer.WriteArrayEnd();

                writer.WriteObjectEnd();

                writer.FinalizeDocument();
            }
        }

        private void GetRevisions(DocumentsOperationContext context, bool metadataOnly)
        {
            var sw = Stopwatch.StartNew();

            var id = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");
            var before = GetDateTimeQueryString("before", required: false);
            var start = GetStart();
            var pageSize = GetPageSize();

            Document[] revisions = Array.Empty<Document>();
            long count = 0;
            if (before != null)
            {
                var revision = Database.DocumentsStorage.RevisionsStorage.GetRevisionBefore(context, id, before.Value);
                if (revision != null)
                {
                    count = 1;
                    revisions = new[] { revision };
                }
            }
            else
            {
                (revisions, count) = Database.DocumentsStorage.RevisionsStorage.GetRevisions(context, id, start, pageSize);
            }
            

            var actualChangeVector = revisions.Length == 0 ? "" : revisions[0].ChangeVector;

            if (GetStringFromHeaders("If-None-Match") == actualChangeVector)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers["ETag"] = "\"" + actualChangeVector + "\"";

            int loadedRevisionsCount;
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");
                writer.WriteDocuments(context, revisions, metadataOnly, out loadedRevisionsCount);

                writer.WriteComma();

                writer.WritePropertyName("TotalResults");
                writer.WriteInteger(count);
                writer.WriteEndObject();
            }

            AddPagingPerformanceHint(PagingOperationType.Revisions, nameof(GetRevisions), HttpContext.Request.QueryString.Value, loadedRevisionsCount, pageSize, sw.ElapsedMilliseconds);
        }

        [RavenAction("/databases/*/revisions/resolved", "GET", AuthorizationStatus.ValidUser)]
        public Task GetResolvedConflictsSince()
        {
            var since = GetStringQueryString("since", required: false);
            var take = GetIntValueQueryString("take", required: false) ?? 1024;
            var date = Convert.ToDateTime(since).ToUniversalTime();
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");
                var revisions = Database.DocumentsStorage.RevisionsStorage.GetResolvedDocumentsSince(context, date, take);
                writer.WriteDocuments(context, revisions, false, out _);
                writer.WriteEndObject();
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
            {
                revisionsStorage.GetLatestRevisionsBinEntryEtag(context, etag, out var actualChangeVector);
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
                    var revisions = revisionsStorage.GetRevisionsBinEntries(context, etag, pageSize);
                    writer.WriteDocuments(context, revisions, false, out count);

                    writer.WriteEndObject();
                }

                AddPagingPerformanceHint(PagingOperationType.Revisions, nameof(GetRevisionsBin), HttpContext.Request.QueryString.Value, count, pageSize, sw.ElapsedMilliseconds);
            }

            return Task.CompletedTask;
        }
    }
}
