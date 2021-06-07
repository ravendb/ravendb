// -----------------------------------------------------------------------
//  <copyright file="StorageWidget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using Raven.Server.NotificationCenter;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class StorageUsageNotificationSender : AbstractClusterDashboardNotificationSender
    {
        private readonly ClusterDashboardPayloadType _payloadType;
        private readonly DatabasesInfoRetriever _databasesInfoRetriever;

        private readonly TimeSpan _defaultInterval = TimeSpan.FromMinutes(1);

        public StorageUsageNotificationSender(int widgetId, ClusterDashboardPayloadType payloadType, DatabasesInfoRetriever databasesInfoRetriever, ConnectedWatcher watcher, CancellationToken shutdown) : base(widgetId, watcher, shutdown)
        {
            _payloadType = payloadType;
            _databasesInfoRetriever = databasesInfoRetriever;
        }

        protected override TimeSpan NotificationInterval => _defaultInterval;

        protected override AbstractClusterDashboardNotification CreateNotification()
        {
            var drivesUsage = _databasesInfoRetriever.GetDrivesUsage();

            switch (_payloadType)
            {
                case ClusterDashboardPayloadType.Server:
                    return new StorageUsagePayload
                    {
                        Items = drivesUsage.Items
                    };
                case ClusterDashboardPayloadType.Database:
                    return new DatabaseStorageUsagePayload
                    {
                        Items = drivesUsage.Items.SelectMany(x => x.Items).GroupBy(x => x.Database).Select(x => new DatabaseDiskUsage
                        {
                            Database = x.Key,
                            Size = x.Sum(y => y.Size),
                            TempBuffersSize = x.Sum(y => y.TempBuffersSize)
                        }).ToList()
                    };
                default:
                    throw new NotSupportedException($"Unknown payload type: {_payloadType}");
            }
        }
    }
}
