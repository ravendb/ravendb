using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForPostSubscriptionTasksState : AbstractOngoingTasksHandlerProcessorForPostSubscriptionTasksState<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForPostSubscriptionTasksState([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask ToggleSubscriptionTaskStateAsync()
        {
            using (var processor = new OngoingTasksHandlerProcessorForToggleTaskState(RequestHandler))
                await processor.ExecuteAsync();
        }
    }
}
