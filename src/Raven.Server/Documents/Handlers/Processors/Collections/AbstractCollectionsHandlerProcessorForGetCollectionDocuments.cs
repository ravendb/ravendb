using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Collections
{
    internal abstract class AbstractCollectionsHandlerProcessorForGetCollectionDocuments<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractCollectionsHandlerProcessorForGetCollectionDocuments([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask<IAsyncEnumerable<Document>> GetCollectionDocumentsAsync(TOperationContext context, string name, int start, int pageSize);

        protected abstract void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration, long totalDocumentsSizeInBytes);

        public override async ValueTask ExecuteAsync()
        {
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using (var token = RequestHandler.CreateOperationToken())
            {
                var sw = Stopwatch.StartNew();
                var pageSize = RequestHandler.GetPageSize();
                var name = RequestHandler.GetStringQueryString("name");
                var start = RequestHandler.GetStart();

                var documents = await GetCollectionDocumentsAsync(context, name, start, pageSize);

                long numberOfResults;
                long totalDocumentsSizeInBytes;
                
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentsAsync(context, documents, metadataOnly: false, token.Token);
                    writer.WriteEndObject();
                }

                AddPagingPerformanceHint(PagingOperationType.Documents, "Collection", HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
            }
        }
    }
}
