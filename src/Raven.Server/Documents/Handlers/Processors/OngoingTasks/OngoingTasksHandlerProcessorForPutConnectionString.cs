using System.Threading.Tasks;
using JetBrains.Annotations;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForPutConnectionString : AbstractOngoingTasksHandlerProcessorForPutConnectionString<DatabaseRequestHandler>
    {
        public OngoingTasksHandlerProcessorForPutConnectionString([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
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
    }
}
