using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Notifications;

internal class ShardedDatabaseNotificationCenterHandlerProcessorForNotifications : AbstractDatabaseNotificationCenterHandlerProcessorForNotifications<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedDatabaseNotificationCenterHandlerProcessorForNotifications([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractDatabaseNotificationCenter GetNotificationCenter() => RequestHandler.DatabaseContext.NotificationCenter;

    protected override bool SupportsCurrentNode => true;

    protected override bool SupportsOptionalShardNumber => true;

    protected override Task HandleRemoteNodeAsync(ProxyCommand<BlittableJsonReaderObject> command, OperationCancelToken token)
    {
        return TryGetShardNumber(out int shardNumber)
            ? RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token)
            : RequestHandler.DatabaseContext.AllOrchestratorNodesExecutor.ExecuteForNodeAsync(command, command.SelectedNodeTag, token.Token);
    }
}
