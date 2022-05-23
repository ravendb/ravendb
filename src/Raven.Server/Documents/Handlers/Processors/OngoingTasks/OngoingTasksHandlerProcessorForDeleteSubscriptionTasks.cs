using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForDeleteSubscriptionTasks : AbstractOngoingTasksHandlerProcessorForDeleteSubscriptionTasks<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public OngoingTasksHandlerProcessorForDeleteSubscriptionTasks([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask DeleteOngoingTaskAsync()
        {
            using (var processor = new OngoingTasksHandlerProcessorForDeleteOngoingTask(RequestHandler))
                await processor.ExecuteAsync();
        }
    }
}
