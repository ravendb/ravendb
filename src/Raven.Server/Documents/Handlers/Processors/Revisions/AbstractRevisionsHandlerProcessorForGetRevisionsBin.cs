using System.Diagnostics;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Exceptions.Documents.Revisions;
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

        protected abstract ValueTask GetAndWriteRevisionsBinAsync(TOperationContext context, long etag, int pageSize);

        protected abstract bool IsRevisionsConfigured();

        protected abstract void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration,
            long totalDocumentsSizeInBytes);

        public override async ValueTask ExecuteAsync()
        {
            if (IsRevisionsConfigured() == false)
                throw new RevisionsDisabledException();

            var start = RequestHandler.GetLongQueryString("etag", required: false) ?? 0;
            var pageSize = RequestHandler.GetPageSize();

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                await GetAndWriteRevisionsBinAsync(context, start, pageSize);
            }
        }
    }
}
