using JetBrains.Annotations;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Handlers.Processors;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Notifications;

internal class ShardedDatabaseNotificationCenterHandlerProcessorForPostpone : AbstractDatabaseNotificationCenterHandlerProcessorForPostpone<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedDatabaseNotificationCenterHandlerProcessorForPostpone([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractDatabaseNotificationCenter GetNotificationCenter() => RequestHandler.DatabaseContext.NotificationCenter;
}
