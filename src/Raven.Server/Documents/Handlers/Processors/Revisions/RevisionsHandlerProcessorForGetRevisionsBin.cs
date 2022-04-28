using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal class RevisionsHandlerProcessorForGetRevisionsBin : AbstractRevisionsHandlerProcessorForGetRevisionsBin<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public RevisionsHandlerProcessorForGetRevisionsBin([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetAndWriteRevisionsBinAsync(DocumentsOperationContext context, int start, int pageSize)
        {
            using (context.OpenReadTransaction())
            {
                var sw = Stopwatch.StartNew();
                var revisionsStorage = RequestHandler.Database.DocumentsStorage.RevisionsStorage;
                revisionsStorage.GetLatestRevisionsBinEntry(context, start, out var actualChangeVector);
                if (actualChangeVector != null)
                {
                    if (RequestHandler.GetStringFromHeaders("If-None-Match") == actualChangeVector)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                        return;
                    }

                    HttpContext.Response.Headers["ETag"] = "\"" + actualChangeVector + "\"";
                }
                var revisions = revisionsStorage.GetRevisionsBinEntries(context, start, pageSize).ToAsyncEnumerable();

                long count;
                long totalDocumentsSizeInBytes;

                using (var token = RequestHandler.CreateOperationToken())
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    (count, totalDocumentsSizeInBytes) = await writer.WriteDocumentsAsync(context, revisions, metadataOnly: false, token.Token);
                    writer.WriteEndObject();
                }

                RequestHandler.AddPagingPerformanceHint(PagingOperationType.Revisions, "GetRevisionsBin", HttpContext.Request.QueryString.Value, count, pageSize, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
            }
        }
    }
}
