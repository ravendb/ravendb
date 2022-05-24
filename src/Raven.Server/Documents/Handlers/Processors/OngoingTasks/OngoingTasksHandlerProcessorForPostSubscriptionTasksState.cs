using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForPostSubscriptionTasksState : AbstractOngoingTasksHandlerProcessorForPostSubscriptionTasksState<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForPostSubscriptionTasksState([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
