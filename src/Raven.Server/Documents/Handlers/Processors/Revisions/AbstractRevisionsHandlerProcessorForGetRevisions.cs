using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForGetRevisions<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractRevisionsHandlerProcessorForGetRevisions([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration,
            long totalDocumentsSizeInBytes);

        protected abstract void CheckNotModified(string actualEtag);

        protected abstract ValueTask GetRevisionByChangeVectorAsync(Microsoft.Extensions.Primitives.StringValues changeVectors, bool metadataOnly, CancellationToken token);
        
        protected abstract ValueTask GetRevisionsAsync(bool metadataOnly, CancellationToken token);

        public override async ValueTask ExecuteAsync()
        {
            using (var token = RequestHandler.CreateOperationToken())
            {
                var changeVectors = RequestHandler.GetStringValuesQueryString("changeVector", required: false);
                var metadataOnly = RequestHandler.GetBoolValueQueryString("metadataOnly", required: false) ?? false;
                
                if (changeVectors.Count > 0)
                {
                    await GetRevisionByChangeVectorAsync(changeVectors, metadataOnly, token.Token);
                }
                else
                {
                    //get revisions by id is sent to a single shard on sharded
                    await GetRevisionsAsync(metadataOnly, token.Token);
                }
            }
        }

        protected void WriteRevisionsBlittable(JsonOperationContext context, RevisionsResult documentsToWrite, out long numberOfResults, out long totalDocumentsSizeInBytes)
        {
            numberOfResults = 0;
            totalDocumentsSizeInBytes = 0;
            HttpContext.Response.Headers["Content-Type"] = "binary/blittable-json";

            using (var streamBuffer = new UnmanagedStreamBuffer(context, RequestHandler.ResponseBodyStream()))
            using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedStreamBuffer>(context,
                       null, new BlittableWriter<UnmanagedStreamBuffer>(context, streamBuffer)))
            {
                writer.StartWriteObjectDocument();

                writer.StartWriteObject();
                writer.WritePropertyName(nameof(BlittableArrayResult.Results));

                writer.StartWriteArray();
                foreach (BlittableJsonReaderObject document in documentsToWrite.Results)
                {
                    numberOfResults++;
                    writer.WriteEmbeddedBlittableDocument(document);
                    totalDocumentsSizeInBytes += document.Size;
                }
                writer.WriteArrayEnd();

                writer.WritePropertyName(nameof(BlittableArrayResult.TotalResults));
                writer.WriteValue(documentsToWrite.TotalResults);

                writer.WriteObjectEnd();

                writer.FinalizeDocument();
            }
        }
    }

    public class RevisionsResult
    {
        public BlittableJsonReaderObject[] Results;
        public long TotalResults;
    }
}
