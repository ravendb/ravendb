using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForPostSubscriptionTasksState<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractOngoingTasksHandlerProcessorForPostSubscriptionTasksState([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract ValueTask ToggleSubscriptionTaskStateAsync();

        public override async ValueTask ExecuteAsync()
        {
            // Note: Only Subscription task needs User authentication, All other tasks need Admin authentication
            var typeStr = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", nameof(type));

            if (type != OngoingTaskType.Subscription)
                throw new ArgumentException("Only Subscription type can call this method");

            await ToggleSubscriptionTaskStateAsync();
        }
    }
}
