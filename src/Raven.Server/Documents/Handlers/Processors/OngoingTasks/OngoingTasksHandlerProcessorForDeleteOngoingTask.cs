using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForDeleteOngoingTask : AbstractOngoingTasksHandlerProcessorForDeleteOngoingTask<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForDeleteOngoingTask([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask RaiseNotificationForSubscriptionTaskRemoval()
        {
            RequestHandler.Database.SubscriptionStorage.RaiseNotificationForTaskRemoved(TaskName);
            return ValueTask.CompletedTask;
        }
    }
}
