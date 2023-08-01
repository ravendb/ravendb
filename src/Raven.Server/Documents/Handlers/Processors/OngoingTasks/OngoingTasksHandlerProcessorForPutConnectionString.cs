using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal sealed class OngoingTasksHandlerProcessorForPutConnectionString : AbstractOngoingTasksHandlerProcessorForPutConnectionString<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForPutConnectionString([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }
    }
}
