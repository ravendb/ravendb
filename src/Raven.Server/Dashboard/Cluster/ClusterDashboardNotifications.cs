using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Dashboard.Cluster.Notifications;
using Raven.Server.NotificationCenter;

namespace Raven.Server.Dashboard.Cluster
{
    public class ClusterDashboardNotifications : NotificationsBase
    {
        private readonly RavenServer _server;
        private readonly CancellationToken _shutdown;
        private readonly DatabasesInfoRetriever _databasesInfoRetriever;

        public ClusterDashboardNotifications(RavenServer server, CanAccessDatabase canAccessDatabase, CancellationToken shutdown)
        {
            _server = server;
            _shutdown = shutdown;
            _databasesInfoRetriever = new DatabasesInfoRetriever(server.ServerStore, canAccessDatabase);
        }

        public async Task<AbstractClusterDashboardNotificationSender> CreateNotificationSender(int topicId, ClusterDashboardNotificationType type)
        {
            var watcher = await EnsureWatcher(); // in current impl we have only one watcher

            switch (type)
            {
                case ClusterDashboardNotificationType.CpuUsage:
                    return new CpuUsageNotificationSender(topicId, _server, watcher, _shutdown);
                case ClusterDashboardNotificationType.MemoryUsage:
                    return new MemoryUsageNotificationSender(topicId, _server, watcher, _shutdown);
                case ClusterDashboardNotificationType.StorageUsage:
                    return new StorageUsageNotificationSender(topicId, ClusterDashboardPayloadType.Server, _databasesInfoRetriever, watcher, _shutdown);
                case ClusterDashboardNotificationType.DatabaseStorageUsage:
                    return new StorageUsageNotificationSender(topicId, ClusterDashboardPayloadType.Database, _databasesInfoRetriever, watcher, _shutdown);
                case ClusterDashboardNotificationType.Traffic:
                    return new TrafficNotificationSender(topicId, ClusterDashboardPayloadType.Server, _databasesInfoRetriever, watcher, _shutdown);
                case ClusterDashboardNotificationType.DatabaseTraffic:
                    return new TrafficNotificationSender(topicId, ClusterDashboardPayloadType.Database, _databasesInfoRetriever, watcher, _shutdown);
                case ClusterDashboardNotificationType.Indexing:
                    return new IndexingSpeedNotificationSender(topicId, ClusterDashboardPayloadType.Server, _databasesInfoRetriever, watcher, _shutdown);
                case ClusterDashboardNotificationType.DatabaseIndexing:
                    return new IndexingSpeedNotificationSender(topicId, ClusterDashboardPayloadType.Database, _databasesInfoRetriever, watcher, _shutdown);
                default:
                    throw new NotSupportedException($"Unsupported cluster dashboard notification type: {type}");
            }
        }


        public async Task<ConnectedWatcher> EnsureWatcher()
        {
            while (true)
            {
                if (Watchers.Count >= 1)
                    return Watchers.First();

                await Task.Delay(200, _shutdown);
            }
        }
    }
}
