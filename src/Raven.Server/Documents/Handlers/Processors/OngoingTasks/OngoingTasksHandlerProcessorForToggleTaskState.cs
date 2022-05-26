using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForToggleTaskState : AbstractOngoingTasksHandlerProcessorForToggleTaskState<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForToggleTaskState([NotNull] DatabaseRequestHandler requestHandler, bool requireAdmin) : base(requestHandler)
        {
            RequireAdmin = requireAdmin;
        }

        protected override bool RequireAdmin { get; }
    }
}
