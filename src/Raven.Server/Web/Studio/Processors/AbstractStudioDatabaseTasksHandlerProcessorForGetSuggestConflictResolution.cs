using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Processors
{
    internal abstract class AbstractStudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext 
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractStudioDatabaseTasksHandlerProcessorForGetSuggestConflictResolution([NotNull] TRequestHandler requestHandler) : base(requestHandler)
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
