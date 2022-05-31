using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.Web.Http;
using Raven.Server.Web.System;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForGetOngoingTaskInfo : ShardedOngoingTasksHandlerProcessorForGetOngoingTasksInfo
    {
        public ShardedOngoingTasksHandlerProcessorForGetOngoingTaskInfo([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override ValueTask HandleCurrentNodeAsync() => throw new System.NotImplementedException();
        
        protected override Task HandleRemoteNodeAsync(ProxyCommand<OngoingTasksResult> command, OperationCancelToken token)
        {
            var shardNumber = GetShardNumber();

            return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
        }

        public override async ValueTask ExecuteAsync()
        {
            await GetOngoingTaskInfoInternalAsync();
        }

        protected override bool SupportsCurrentNode => false;
    }
}
