using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
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

        protected abstract ValueTask<(long numberOfResults, long totalDocumentsSizeInBytes)> GetCollectionDocumentsAndWriteAsync(TOperationContext context, string name, int start, int pageSize, CancellationToken token);
        
        public override async ValueTask ExecuteAsync()
        {
            var pageSize = RequestHandler.GetPageSize();
            var name = RequestHandler.GetStringQueryString("name");
            var start = RequestHandler.GetStart();

            var sw = Stopwatch.StartNew();
            long numberOfResults, totalDocumentsSizeInBytes;
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            using (var token = RequestHandler.CreateOperationToken())
            {
                (numberOfResults, totalDocumentsSizeInBytes) = await GetCollectionDocumentsAndWriteAsync(context, name, start, pageSize, token.Token);
            }

            RequestHandler.AddPagingPerformanceHint(PagingOperationType.Documents, "Collection", HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
        }
    }
}
