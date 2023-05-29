using System.Threading;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;

namespace Raven.Server.Dashboard;

public class ThreadsInfoNotifications : NotificationsBase
{
    public ThreadsInfoNotifications(CancellationToken shutdown)
    {
        var options = new ThreadsInfoOptions();

        var threadsInfoNotificationSender = new ThreadsInfoNotificationSender(Watchers, options.ThreadsInfoThrottle, shutdown);
        BackgroundWorkers.Add(threadsInfoNotificationSender);
    }
}
