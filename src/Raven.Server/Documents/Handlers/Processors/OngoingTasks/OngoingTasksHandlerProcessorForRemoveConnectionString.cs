using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal sealed class OngoingTasksHandlerProcessorForRemoveConnectionString : AbstractOngoingTasksHandlerProcessorForRemoveConnectionString<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForRemoveConnectionString([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
