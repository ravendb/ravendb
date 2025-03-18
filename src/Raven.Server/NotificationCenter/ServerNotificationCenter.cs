using Raven.Server.Logging;
using Raven.Server.NotificationCenter.BackgroundWork;
using Raven.Server.ServerWide;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter;

public sealed class ServerNotificationCenter : AbstractNotificationCenter
{
    public ServerNotificationCenter(ServerStore serverStore, NotificationsStorage storage)
        : base(storage, serverStore.Configuration, RavenLogManager.Instance.GetLoggerForServer<ServerNotificationCenter>())
    {
        PostponedNotificationSender = new PostponedNotificationsSender(resourceName: null, Storage, Watchers, RavenLogManager.Instance.GetLoggerForServer<PostponedNotificationsSender>(), serverStore.ServerShutdown);
    }

    protected override PostponedNotificationsSender PostponedNotificationSender { get; }
}
