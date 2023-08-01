using System.Threading;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;

namespace Raven.Server.Dashboard;

public sealed class ThreadsInfoNotifications : NotificationsBase
{
    public ThreadsInfoNotifications(CancellationToken shutdown)
    {
        var options = new ThreadsInfoOptions();

        var threadsInfoNotificationSender = new ThreadsInfoNotificationSender(nameof(ServerStore), Watchers, options.ThreadsInfoThrottle, shutdown);
        BackgroundWorkers.Add(threadsInfoNotificationSender);
    }
}
