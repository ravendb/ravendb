using System.Threading.Tasks;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForDeleteSubscriptionTasks<TRequestHandler, TOperationContext> : AbstractOngoingTasksHandlerProcessorForDeleteOngoingTask<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractOngoingTasksHandlerProcessorForDeleteSubscriptionTasks([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        public override async ValueTask ExecuteAsync()
        {
            // Note: Only Subscription task needs User authentication, All other tasks need Admin authentication
            var typeStr = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            OngoingTasksHandlerProcessorForPostSubscriptionTasksState.ValidateSubscriptionTaskType(typeStr);
            await base.ExecuteAsync();
        }
    }
}
