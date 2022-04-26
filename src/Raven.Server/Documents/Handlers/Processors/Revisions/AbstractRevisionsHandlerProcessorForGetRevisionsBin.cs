using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForGetRevisionsBin<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        public AbstractRevisionsHandlerProcessorForGetRevisionsBin([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract ValueTask GetAndWriteRevisionsBinAsync(TOperationContext context, int start, int pageSize);
        
        protected abstract void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration,
            long totalDocumentsSizeInBytes);

        public override async ValueTask ExecuteAsync()
        {
            if (RequestHandler.GetLongQueryString("etag", required: false).HasValue)
                throw new NotSupportedException("Parameter 'etag' is deprecated for endpoint /databases/*/revisions/bin. Use 'start' instead.");
            
            var start = RequestHandler.GetStart();
            var pageSize = RequestHandler.GetPageSize();

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                await GetAndWriteRevisionsBinAsync(context, start, pageSize);
            }
        }
    }
}
