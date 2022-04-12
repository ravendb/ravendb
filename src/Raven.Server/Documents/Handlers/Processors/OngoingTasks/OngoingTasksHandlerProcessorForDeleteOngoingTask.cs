using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForDeleteOngoingTask : AbstractOngoingTasksHandlerProcessorForDeleteOngoingTask<DatabaseRequestHandler>
    {
        public OngoingTasksHandlerProcessorForDeleteOngoingTask([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override string GetDatabaseName()
        {
            return RequestHandler.Database.Name;
        }

        protected override async ValueTask WaitForIndexNotificationAsync(long index)
        {
            await RequestHandler.Database.RachisLogIndexNotifications.WaitForIndexNotification(index, RequestHandler.ServerStore.Engine.OperationTimeout);
        }

        protected override ValueTask RaiseNotificationForSubscriptionTaskRemoval()
        {
            RequestHandler.Database.SubscriptionStorage.RaiseNotificationForTaskRemoved(TaskName);
            return ValueTask.CompletedTask;
        }
    }
}
