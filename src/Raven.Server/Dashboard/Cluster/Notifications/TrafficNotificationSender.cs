// -----------------------------------------------------------------------
//  <copyright file="TrafficWidget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using Raven.Server.NotificationCenter;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class TrafficNotificationSender : AbstractClusterDashboardNotificationSender
    {
        private readonly ClusterDashboardPayloadType _payloadType;
        private readonly DatabasesInfoRetriever _databasesInfoRetriever;

        public TrafficNotificationSender(int widgetId, ClusterDashboardPayloadType payloadType, DatabasesInfoRetriever databasesInfoRetriever, ConnectedWatcher watcher, CancellationToken shutdown) : base(widgetId, watcher, shutdown)
        {
            _payloadType = payloadType;
            _databasesInfoRetriever = databasesInfoRetriever;
        }

        protected override TimeSpan NotificationInterval => DatabasesInfoRetriever.RefreshRate;

        protected override AbstractClusterDashboardNotification CreateNotification()
        {
            var trafficWatch = _databasesInfoRetriever.GetTrafficWatch();

            switch (_payloadType)
            {
                case ClusterDashboardPayloadType.Server:
                    return new TrafficWatchPayload
                    {
                        TrafficPerDatabase = trafficWatch.Items
                    };
                case ClusterDashboardPayloadType.Database:
                    return new DatabaseTrafficWatchPayload
                    {
                        Items = trafficWatch.Items
                    };
                default:
                    throw new NotSupportedException($"Unknown payload type: {_payloadType}");
            }
        }
    }
}
