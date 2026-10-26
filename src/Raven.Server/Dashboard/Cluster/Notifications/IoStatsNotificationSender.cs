using System;
using System.Linq;
using System.Threading;
using Raven.Server.NotificationCenter;
using Raven.Server.Storage;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public sealed class IoStatsNotificationSender : AbstractClusterDashboardNotificationSender
    {
        private readonly DatabasesInfoRetriever _databasesInfoRetriever;
        private readonly StorageDeviceMonitor _storageDeviceMonitor;

        private readonly TimeSpan _defaultInterval = TimeSpan.FromSeconds(1);

        public IoStatsNotificationSender(int widgetId, DatabasesInfoRetriever databasesInfoRetriever, StorageDeviceMonitor storageDeviceMonitor, ConnectedWatcher watcher,
            CancellationToken shutdown) : base(widgetId, watcher, shutdown)
        {
            _databasesInfoRetriever = databasesInfoRetriever;
            _storageDeviceMonitor = storageDeviceMonitor;
        }

        protected override TimeSpan NotificationInterval => _defaultInterval;

        protected override AbstractClusterDashboardNotification CreateNotification()
        {
            var drivesUsage = _databasesInfoRetriever.GetDrivesUsage();

            return new IoStatsPayload
            {
                Items = drivesUsage.Items.Select(x => x.IoStatsResult).Where(x => x != null).ToList(),
                StorageStatus = StorageStatusInfo.Create(_storageDeviceMonitor)
            };
        }
    }
}
