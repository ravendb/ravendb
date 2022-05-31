using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.Web.Http;
using NotImplementedException = System.NotImplementedException;

namespace Raven.Server.Web.System.Processors.OngoingTasks
{
    internal class OngoingTasksHandlerProcessorForGetOngoingTaskInfo : OngoingTasksHandlerProcessorForGetOngoingTasksInfo
    {
        public OngoingTasksHandlerProcessorForGetOngoingTaskInfo([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            await GetOngoingTaskInfoInternalAsync();
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<OngoingTasksResult> command, OperationCancelToken token) => throw new NotImplementedException();
        
        protected override bool SupportsCurrentNode => true;
    }
}
