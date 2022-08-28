using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.ServerWide;
using Raven.Server.Web.Http;
using Raven.Server.Web.System;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForGetOngoingTasks : ShardedOngoingTasksHandlerProcessorForGetOngoingTasksInfo
    {
        public ShardedOngoingTasksHandlerProcessorForGetOngoingTasks([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            var result = GetOngoingTasksInternal();

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }

        protected override RavenCommand<OngoingTasksResult> CreateCommandForNode(string nodeTag)
        {
            return new GetOngoingTasksInfoCommand(nodeTag);
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<OngoingTasksResult> command, OperationCancelToken token) => RequestHandler.DatabaseContext.AllNodesExecutor.ExecuteForNodeAsync(command, command.SelectedNodeTag, token.Token);
    }

    internal class GetOngoingTasksInfoCommand : RavenCommand<OngoingTasksResult>
    {
        public GetOngoingTasksInfoCommand(string nodeTag)
        {
            SelectedNodeTag = nodeTag;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/tasks";

            var request = new HttpRequestMessage { Method = HttpMethod.Get };

            return request;
        }

        public override bool IsReadRequest => true;
    }
}
