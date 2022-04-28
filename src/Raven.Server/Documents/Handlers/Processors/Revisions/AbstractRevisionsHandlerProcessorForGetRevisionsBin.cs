using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForGetRevisionsBin<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractRevisionsHandlerProcessorForGetRevisionsBin([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask GetAndWriteRevisionsBinAsync(TOperationContext context, int start, int pageSize);
        
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
