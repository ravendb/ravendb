using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForDeleteSubscriptionTasks : AbstractOngoingTasksHandlerProcessorForDeleteSubscriptionTasks<DatabaseRequestHandler,
        DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForDeleteSubscriptionTasks([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
        
        protected override ValueTask RaiseNotificationForSubscriptionTaskRemoval()
        {
            RequestHandler.Database.SubscriptionStorage.RaiseNotificationForTaskRemoved(TaskName);
            return ValueTask.CompletedTask;
        }
    }
}
