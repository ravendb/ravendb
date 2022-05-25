using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForPostSubscriptionTasksState<TRequestHandler, TOperationContext> : AbstractOngoingTasksHandlerProcessorForToggleTaskState<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        public AbstractOngoingTasksHandlerProcessorForPostSubscriptionTasksState([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, object _, string raftRequestId)
        {
            // Note: Only Subscription task needs User authentication, All other tasks need Admin authentication
            var typeStr = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");
            ValidateSubscriptionTaskType(typeStr);
            return base.OnUpdateConfiguration(context, _, raftRequestId);
        }

        public static void ValidateSubscriptionTaskType(string typeStr)
        {
            if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
                throw new ArgumentException($"Unknown task type: {type}", nameof(type));

            if (type != OngoingTaskType.Subscription)
                throw new ArgumentException("Only Subscription type can call this method");
        }
    }
}
