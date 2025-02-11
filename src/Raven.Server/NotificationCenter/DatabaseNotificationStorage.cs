using Raven.Server.Rachis.Commands;
using Raven.Server.ServerWide;

namespace Raven.Server.NotificationCenter;

public sealed class DatabaseNotificationStorage(ServerStore serverStore, string resourceName) : NotificationsStorage(serverStore, resourceName)
{
    protected override void CreateSchema()
    {
        var command = new InitializeSchemaForNotificationsCommand(TableName);
        serverStore.Engine.TxMerger.EnqueueSync(command);
    }
}
