using System;
using System.Threading;
using Raven.Server.NotificationCenter;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class OngoingTasksNotificationSender : AbstractClusterDashboardNotificationSender
    {
        private readonly DatabasesInfoRetriever _databasesInfoRetriever;

        public OngoingTasksNotificationSender(int widgetId, DatabasesInfoRetriever databasesInfoRetriever, ConnectedWatcher watcher, CancellationToken shutdown) : base(widgetId, watcher, shutdown)
        {
            _databasesInfoRetriever = databasesInfoRetriever;
        }

        protected override TimeSpan NotificationInterval => DatabasesInfoRetriever.RefreshRate;
        protected override AbstractClusterDashboardNotification CreateNotification()
        {
            var databasesOngoingTasksInfo = _databasesInfoRetriever.GetDatabasesOngoingTasksInfo();

            return new OngoingTasksPayload
            {
                Items = databasesOngoingTasksInfo.Items
            };
        }
    }
}
