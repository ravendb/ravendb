using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.ServerWide;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter;

public class ServerNotificationCenter : AbstractNotificationCenter
{
    public ServerNotificationCenter(ServerStore serverStore, NotificationsStorage storage)
        : base(storage, serverStore.Configuration, LoggingSource.Instance.GetLogger<ServerNotificationCenter>("Server"))
    {
        PostponedNotificationSender = new PostponedNotificationsSender(resourceName: null, Storage, Watchers, serverStore.ServerShutdown);
    }

    protected override PostponedNotificationsSender PostponedNotificationSender { get; }
}
