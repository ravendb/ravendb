using System.Threading;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;

namespace Raven.Server.Dashboard
{
    public class ServerDashboardNotifications : NotificationsBase
    {
        public ServerDashboardNotifications(ServerStore serverStore, CancellationToken shutdown)
        {
            var options = new ServerDashboardOptions();

            var machineResourcesNotificationSender = 
                new MachineResourcesNotificationSender(serverStore.Server, Watchers, options.MachineResourcesThrottle, shutdown);
            BackgroundWorkers.Add(machineResourcesNotificationSender);

            var databasesInfoNotificationSender = 
                new DatabasesInfoNotificationSender(serverStore, Watchers, options.DatabasesInfoThrottle, shutdown);
            BackgroundWorkers.Add(databasesInfoNotificationSender);
        }
    }
}
