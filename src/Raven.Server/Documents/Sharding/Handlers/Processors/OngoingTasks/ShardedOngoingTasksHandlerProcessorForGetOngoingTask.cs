using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Raven.Server.Web.System.Processors.OngoingTasks;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks
{
    internal sealed class ShardedOngoingTasksHandlerProcessorForGetOngoingTask : AbstractOngoingTasksHandlerProcessorForGetOngoingTask<ShardedDatabaseRequestHandler, TransactionOperationContext, SubscriptionConnectionsStateOrchestrator>
    {
        public ShardedOngoingTasksHandlerProcessorForGetOngoingTask([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.DatabaseContext.OngoingTasks)
        {
        }

        protected override bool SupportsOptionalShardNumber => true;

        protected override Task HandleRemoteNodeAsync(ProxyCommand<OngoingTask> command, OperationCancelToken token)
        {
            return TryGetShardNumber(out int shardNumber)
                ? RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token)
                : RequestHandler.DatabaseContext.AllOrchestratorNodesExecutor.ExecuteForNodeAsync(command, command.SelectedNodeTag, token.Token);
        }
    }
}
