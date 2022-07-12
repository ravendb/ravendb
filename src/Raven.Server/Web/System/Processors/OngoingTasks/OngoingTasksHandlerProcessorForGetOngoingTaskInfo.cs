using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks;
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

        protected override Task HandleRemoteNodeAsync(ProxyCommand<OngoingTasksResult> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
        
        protected override RavenCommand<OngoingTasksResult> CreateCommandForNode(string nodeTag)
        {
            var (taskId, taskName, type) = TryGetParameters();
            return new GetOngoingTaskInfoCommand(taskId, taskName, type);
        }
        
        
        protected override bool SupportsCurrentNode => true;
    }
}
