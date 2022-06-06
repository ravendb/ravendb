using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Notifications;

internal class ShardedDatabaseNotificationCenterHandlerProcessorForPostpone : AbstractDatabaseNotificationCenterHandlerProcessorForPostpone<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedDatabaseNotificationCenterHandlerProcessorForPostpone([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractDatabaseNotificationCenter GetNotificationCenter() => RequestHandler.DatabaseContext.NotificationCenter;

    protected override bool SupportsCurrentNode => true;

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token)
    {
        return TryGetShardNumber(out int shardNumber)
            ? RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token)
            : RequestHandler.DatabaseContext.AllNodesExecutor.ExecuteForNodeAsync(command, command.SelectedNodeTag, token.Token);
    }
}
