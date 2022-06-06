using JetBrains.Annotations;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Handlers.Processors;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Notifications;

internal class ShardedDatabaseNotificationCenterHandlerProcessorForDismiss : AbstractDatabaseNotificationCenterHandlerProcessorForDismiss<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedDatabaseNotificationCenterHandlerProcessorForDismiss([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractDatabaseNotificationCenter GetNotificationCenter() => RequestHandler.DatabaseContext.NotificationCenter;
}
