using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Web.System;
using Raven.Server.Web.System.Processors.OngoingTasks;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal class ShardedOngoingTasksHandlerProcessorForGetOngoingTasks : AbstractOngoingTasksHandlerProcessorForGetOngoingTasks<ShardedDatabaseRequestHandler, TransactionOperationContext, SubscriptionConnectionsStateOrchestrator>
    {
        public ShardedOngoingTasksHandlerProcessorForGetOngoingTasks([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.DatabaseContext.OngoingTasks)
        {
        }

        protected override long SubscriptionsCount => RequestHandler.DatabaseContext.SubscriptionsStorage.GetAllSubscriptionsCount();

        protected override bool IsCurrentNode(out string nodeTag)
        {
            var isCurrentNode = base.IsCurrentNode(out nodeTag);
            if (isCurrentNode)
            {
                if (TryGetShardNumber(out _))
                    return false;
            }

            return isCurrentNode;
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<OngoingTasksResult> command, OperationCancelToken token)
        {
            return TryGetShardNumber(out int shardNumber) 
                ? RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token) 
                : RequestHandler.DatabaseContext.AllOrchestratorNodesExecutor.ExecuteForNodeAsync(command, command.SelectedNodeTag, token.Token);
        }
    }
}
