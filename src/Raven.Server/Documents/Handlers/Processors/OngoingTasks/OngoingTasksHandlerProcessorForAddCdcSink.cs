using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForAddCdcSink : AbstractOngoingTasksHandlerProcessorForAddCdcSink<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForAddCdcSink([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
