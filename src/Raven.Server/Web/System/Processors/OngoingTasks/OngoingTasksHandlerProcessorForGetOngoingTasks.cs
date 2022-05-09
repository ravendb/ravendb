using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForGetOngoingTasks : OngoingTasksHandlerProcessorForGetOngoingTasksInfo
    {
        public OngoingTasksHandlerProcessorForGetOngoingTasks([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        public override async ValueTask ExecuteAsync()
        {
            var result = GetOngoingTasksInternal();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }
    }
}
