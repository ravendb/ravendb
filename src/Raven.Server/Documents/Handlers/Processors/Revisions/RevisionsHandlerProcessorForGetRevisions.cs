using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal class RevisionsHandlerProcessorForGetRevisions : AbstractRevisionsHandlerProcessorForGetRevisions<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RevisionsHandlerProcessorForGetRevisions([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetRevisionByChangeVectorAsync(DocumentsOperationContext context, StringValues changeVectors, bool metadataOnly, CancellationToken token)
        {
            using (context.OpenReadTransaction())
            {
                var revisionsStorage = RequestHandler.Database.DocumentsStorage.RevisionsStorage;
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
                if (NotModified(actualEtag))
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                if (string.IsNullOrEmpty(actualEtag) == false)
                    HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + actualEtag + "\"";

                long numberOfResults;
                long totalDocumentsSizeInBytes;
                var blittable = RequestHandler.GetBoolValueQueryString("blittable", required: false) ?? false;
                if (blittable)
                {
                    WriteRevisionsBlittable(context, GetDataArrayFromDocumentList(revisions), out numberOfResults, out totalDocumentsSizeInBytes);
                }
                else
                {
                    (numberOfResults, totalDocumentsSizeInBytes) = await WriteRevisionsJsonAsync(context, metadataOnly, revisions, token);
                }

                //using this function's legacy name GetRevisionByChangeVector
                RequestHandler.AddPagingPerformanceHint(PagingOperationType.Documents, "GetRevisionByChangeVector", HttpContext.Request.QueryString.Value, numberOfResults,
                    revisions.Count, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
            }
        }

        protected override async ValueTask GetRevisionsAsync(DocumentsOperationContext context, string id, DateTime? before, int start, int pageSize, bool metadataOnly, CancellationToken token)
        {
            using (context.OpenReadTransaction())
            {
                var sw = Stopwatch.StartNew();

                Document[] revisions = Array.Empty<Document>();
                long count = 0;
                if (before != null)
                {
                    var revision = RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetRevisionBefore(context, id, before.Value);
                    if (revision != null)
                    {
                        count = 1;
                        revisions = new[] {revision};
                    }
                }
                else
                {
                    (revisions, count) = RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetRevisions(context, id, start, pageSize);
                }

                var actualChangeVector = revisions.Length == 0 ? "" : revisions[0].ChangeVector;
                if (NotModified(actualChangeVector))
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }
                
                if(string.IsNullOrEmpty(actualChangeVector) == false)
                    HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + actualChangeVector + "\"";
                
                long loadedRevisionsCount;
                long totalDocumentsSizeInBytes;
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(RevisionsResult<Document>.Results));
                    (loadedRevisionsCount, totalDocumentsSizeInBytes) = await writer.WriteDocumentsAsync(context, revisions, metadataOnly, token);

                    writer.WriteComma();

                    writer.WritePropertyName(nameof(RevisionsResult<Document>.TotalResults));
                    writer.WriteInteger(count);
                    writer.WriteEndObject();
                }

                //using this function's legacy name GetRevisions
                RequestHandler.AddPagingPerformanceHint(PagingOperationType.Revisions, "GetRevisions", HttpContext.Request.QueryString.Value, loadedRevisionsCount, pageSize,
                    sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
            }
        }

        protected override bool NotModified(string actualEtag)
        {
            var etag = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);
            if (etag == actualEtag)
                return true;
            
            return false;
        }

        protected async ValueTask<(long NumberOfResults, long TotalDocumentsSizeInBytes)> WriteRevisionsJsonAsync(JsonOperationContext context, bool metadataOnly, List<Document> revisionsResult, CancellationToken token)
        {
            long numberOfResults;
            long totalDocumentsSizeInBytes;
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(RevisionsResult<Document>.Results));
                (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentsAsync(context, revisionsResult, metadataOnly, token);
                writer.WriteEndObject();
            }

            return (numberOfResults, totalDocumentsSizeInBytes);
        }

        private static BlittableJsonReaderObject[] GetDataArrayFromDocumentList(List<Document> documents)
        {
            var dataArr = new BlittableJsonReaderObject[documents.Count];
            int i = 0;
            foreach (var doc in documents)
            {
                dataArr[i] = doc.Data;
                i++;
            }

            return dataArr;
        }

    }
}
