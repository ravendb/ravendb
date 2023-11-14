using JetBrains.Annotations;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal sealed class OngoingTasksHandlerProcessorForToggleTaskState : AbstractOngoingTasksHandlerProcessorForToggleTaskState<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForToggleTaskState([NotNull] DatabaseRequestHandler requestHandler, bool requireAdmin) : base(requestHandler)
        {
            RequireAdmin = requireAdmin;
        }

        protected override bool RequireAdmin { get; }
        protected override AbstractSubscriptionStorage GetSubscriptionStorage()
        {
            return RequestHandler.Database.SubscriptionStorage;
        }
    }
}
