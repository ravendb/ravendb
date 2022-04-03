using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal abstract class AbstractReplicationHandlerProcessorForGetConflicts<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractReplicationHandlerProcessorForGetConflicts([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
            : base(requestHandler, contextPool)
        {
        }

        protected abstract Task GetConflictsByEtagAsync(TOperationContext context, long etag);

        protected abstract Task GetConflictsForDocumentAsync(TOperationContext context, string documentId);

        public override async ValueTask ExecuteAsync()
        {
            var docId = RequestHandler.GetStringQueryString("docId", required: false);
            var etag = RequestHandler.GetLongQueryString("etag", required: false) ?? 0;

            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                if (string.IsNullOrWhiteSpace(docId))
                    await GetConflictsByEtagAsync(context, etag);

                else
                    await GetConflictsForDocumentAsync(context, docId);
            }
        }
    }
}
