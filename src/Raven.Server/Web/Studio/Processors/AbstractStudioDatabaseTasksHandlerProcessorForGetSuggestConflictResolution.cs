using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors
{
    internal abstract class AbstractStudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : RequestHandler
        where TOperationContext : JsonOperationContext
    {
        protected AbstractStudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
        {
        }

        protected abstract Task GetSuggestConflictResolutionAsync(TOperationContext context, string documentId);

        public override async ValueTask ExecuteAsync()
        {
            var docId = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("docId");
            using (ContextPool.AllocateOperationContext(out TOperationContext context))
            {
                await GetSuggestConflictResolutionAsync(context, docId);
            }
        }
    }
}
