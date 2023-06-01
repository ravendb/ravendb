using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForAddQueueSink : AbstractOngoingTasksHandlerProcessorForAddQueueSink<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForAddQueueSink([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
