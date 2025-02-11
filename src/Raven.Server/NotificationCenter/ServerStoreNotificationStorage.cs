using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.NotificationCenter;

public sealed class ServerStoreNotificationStorage(ServerStore serverStore) : NotificationsStorage(serverStore)
{
    protected override void CreateSchema()
    {
        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = Environment.WriteTransaction(context.PersistentContext))
        {
            Documents.Schemas.Notifications.Current.Create(tx, TableName, 16);
            tx.Commit();
        }
    }
}
