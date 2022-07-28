using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Collections
{
    internal class CollectionsHandlerProcessorForGetCollectionDocuments : AbstractCollectionsHandlerProcessorForGetCollectionDocuments<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public CollectionsHandlerProcessorForGetCollectionDocuments([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<(long numberOfResults, long totalDocumentsSizeInBytes)> GetCollectionDocumentsAndWriteAsync(DocumentsOperationContext context, string name, int start, int pageSize, CancellationToken token)
        {
            using (context.OpenReadTransaction())
            {
                var documents = RequestHandler.Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, name, start, pageSize).ToAsyncEnumerable();

                long numberOfResults;
                long totalDocumentsSizeInBytes;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentsAsync(context, documents, metadataOnly: false, token);
                    writer.WriteEndObject();
                }

                return (numberOfResults, totalDocumentsSizeInBytes);
            }
        }
    }
}
