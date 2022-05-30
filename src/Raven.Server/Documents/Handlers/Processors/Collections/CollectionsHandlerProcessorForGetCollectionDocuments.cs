using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Collections
{
    internal class CollectionsHandlerProcessorForGetCollectionDocuments : AbstractCollectionsHandlerProcessorForGetCollectionDocuments<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public CollectionsHandlerProcessorForGetCollectionDocuments([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask<IAsyncEnumerable<Document>> GetCollectionDocumentsAsync(DocumentsOperationContext context, string name, int start, int pageSize)
        {
            using (context.OpenReadTransaction())
            {
                return ValueTask.FromResult(RequestHandler.Database.DocumentsStorage.GetDocumentsInReverseEtagOrder(context, name, start, pageSize).ToAsyncEnumerable());
            }
        }

        protected override void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration,
            long totalDocumentsSizeInBytes)
        {
            RequestHandler.AddPagingPerformanceHint(operation, action, details, numberOfResults, pageSize, duration, totalDocumentsSizeInBytes);
        }
    }
}
