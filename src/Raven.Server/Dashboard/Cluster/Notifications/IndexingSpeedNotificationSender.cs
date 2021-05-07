using System;
using System.Threading;
using Raven.Server.NotificationCenter;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class IndexingSpeedNotificationSender : AbstractClusterDashboardNotificationSender
    {
        private readonly ClusterDashboardPayloadType _payloadType;
        private readonly DatabasesInfoRetriever _databasesInfoRetriever;

        public IndexingSpeedNotificationSender(int widgetId, ClusterDashboardPayloadType payloadType, DatabasesInfoRetriever databasesInfoRetriever, ConnectedWatcher watcher, CancellationToken shutdown) : base(widgetId, watcher, shutdown)
        {
            _payloadType = payloadType;
            _databasesInfoRetriever = databasesInfoRetriever;
        }

        protected override TimeSpan NotificationInterval => DatabasesInfoRetriever.RefreshRate;
        protected override AbstractClusterDashboardNotification CreateNotification()
        {
            var indexingSpeed = _databasesInfoRetriever.GetIndexingSpeed();

            switch (_payloadType)
            {
                case ClusterDashboardPayloadType.Server:
                    return new IndexingSpeedPayload
                    {
                        IndexingSpeedPerDatabase = indexingSpeed.Items
                    };
                case ClusterDashboardPayloadType.Database:
                    return new DatabaseIndexingSpeedPayload
                    {
                        Items = indexingSpeed.Items
                    };
                default:
                    throw new NotSupportedException($"Unknown payload type: {_payloadType}");
            }
        }
    }
}
