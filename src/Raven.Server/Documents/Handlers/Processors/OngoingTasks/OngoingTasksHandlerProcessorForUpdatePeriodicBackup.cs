using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForUpdatePeriodicBackup : AbstractOngoingTasksHandlerProcessorForUpdatePeriodicBackup<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForUpdatePeriodicBackup([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
