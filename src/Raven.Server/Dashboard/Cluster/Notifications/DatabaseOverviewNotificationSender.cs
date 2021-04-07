using System;
using System.Threading;
using Raven.Server.NotificationCenter;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class DatabaseOverviewNotificationSender : AbstractClusterDashboardNotificationSender
    {
        private readonly DatabasesInfoRetriever _databasesInfoRetriever;

        public DatabaseOverviewNotificationSender(int widgetId, DatabasesInfoRetriever databasesInfoRetriever, ConnectedWatcher watcher, CancellationToken shutdown) : base(widgetId, watcher, shutdown)
        {
            _databasesInfoRetriever = databasesInfoRetriever;
        }

        protected override TimeSpan NotificationInterval =>_databasesInfoRetriever.RefreshRate;
        protected override AbstractClusterDashboardNotification CreateNotification()
        {
            var databasesInfo = _databasesInfoRetriever.GetDatabasesInfo();

            return new DatabaseOverviewPayload
            {
                Items = databasesInfo.Items
            };
        }
    }
}
