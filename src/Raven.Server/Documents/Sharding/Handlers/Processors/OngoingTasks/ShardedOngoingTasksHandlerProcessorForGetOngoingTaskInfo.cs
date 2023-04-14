using System;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForGetOngoingTaskInfo : ShardedOngoingTasksHandlerProcessorForGetOngoingTasksInfo
    {
        public ShardedOngoingTasksHandlerProcessorForGetOngoingTaskInfo([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask HandleCurrentNodeAsync()
        {
            return HandleOngoingTaskInfoAsync();
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<OngoingTasksResult> command, OperationCancelToken token)
        {
            var shardNumber = GetShardNumber();

            return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
        }

        protected override RavenCommand<OngoingTasksResult> CreateCommandForNode(string nodeTag)
        {
            var (taskId, taskName, type) = TryGetParameters();
            return new GetOngoingTaskInfoCommand(taskId, taskName, type);
        }

        protected override bool SupportsCurrentNode => true;
    }

    internal class GetOngoingTaskInfoCommand : RavenCommand<OngoingTasksResult>
    {
        private readonly string _taskName;
        private readonly long _taskId;
        private readonly OngoingTaskType _type;

        public GetOngoingTaskInfoCommand(long taskId, string taskName, OngoingTaskType type)
        {
            if (string.IsNullOrWhiteSpace(taskName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(taskName));
            _taskName = taskName;
            _type = type;
            _taskId = taskId;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = _taskName != null
                ? $"{node.Url}/databases/{node.Database}/task?taskName={Uri.EscapeDataString(_taskName)}&type={_type}"
                : $"{node.Url}/databases/{node.Database}/task?key={_taskId}&type={_type}";

            var request = new HttpRequestMessage { Method = HttpMethod.Get };

            return request;
        }

        public override bool IsReadRequest => false;
    }
}
