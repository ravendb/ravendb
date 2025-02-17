using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

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

    protected override void Cleanup()
    {
        RemoveNewVersionAvailableAlertIfNecessary();
    }

    private void RemoveNewVersionAvailableAlertIfNecessary()
    {
        var buildNumber = ServerVersion.Build;

        var id = AlertRaised.GetKey(AlertType.Server_NewVersionAvailable, null);
        using (Read(id, out var ntv))
        {
            using (ntv)
            {
                if (ntv == null)
                    return;

                var delete = true;

                if (buildNumber != ServerVersion.DevBuildNumber)
                {
                    if (ntv.Json.TryGetMember(nameof(AlertRaised.Details), out var o)
                        && o is BlittableJsonReaderObject detailsJson)
                    {
                        if (detailsJson.TryGetMember(nameof(NewVersionAvailableDetails.VersionInfo), out o)
                            && o is BlittableJsonReaderObject newVersionDetailsJson)
                        {
                            var value = JsonDeserializationServer.LatestVersionCheckVersionInfo(newVersionDetailsJson);
                            delete = value.BuildNumber <= buildNumber;
                        }
                    }
                }

                if (delete)
                    Delete(id);
            }
        }
    }
}
