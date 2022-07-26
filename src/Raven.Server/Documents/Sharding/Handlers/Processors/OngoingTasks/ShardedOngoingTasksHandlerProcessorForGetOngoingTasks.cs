using System;
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

        protected override ValueTask HandleCurrentNodeAsync() => throw new NotImplementedException();

        protected override Task HandleRemoteNodeAsync(ProxyCommand<OngoingTasksResult> command, OperationCancelToken token)
        {
            var shardNumber = GetShardNumber();

            return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
        }

        protected override RavenCommand<OngoingTasksResult> CreateCommandForNode(string nodeTag)
        {
            return new GetOngoingTasksInfoCommand(nodeTag);
        }

        protected override bool SupportsCurrentNode => false;
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
