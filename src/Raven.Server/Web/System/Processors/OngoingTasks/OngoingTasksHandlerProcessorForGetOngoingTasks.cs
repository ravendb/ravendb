using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.Web.Http;
using Sparrow.Json;
using NotImplementedException = System.NotImplementedException;

namespace Raven.Server.Web.System.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForGetOngoingTasks : OngoingTasksHandlerProcessorForGetOngoingTasksInfo
    {
        public OngoingTasksHandlerProcessorForGetOngoingTasks([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            var result = GetOngoingTasksInternal();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<OngoingTasksResult> command, OperationCancelToken token) => throw new NotImplementedException();
        
        protected override bool SupportsCurrentNode => true;
    }
}
