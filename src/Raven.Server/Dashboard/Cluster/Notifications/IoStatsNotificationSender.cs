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
    public class IoStatsNotificationSender : AbstractClusterDashboardNotificationSender
    {
        private readonly DatabasesInfoRetriever _databasesInfoRetriever;

        private readonly TimeSpan _defaultInterval = TimeSpan.FromSeconds(1);

        public IoStatsNotificationSender(int widgetId, DatabasesInfoRetriever databasesInfoRetriever, ConnectedWatcher watcher, CancellationToken shutdown) : base(widgetId, watcher, shutdown)
        {
            _databasesInfoRetriever = databasesInfoRetriever;
        }

        protected override TimeSpan NotificationInterval => _defaultInterval;

        protected override AbstractClusterDashboardNotification CreateNotification()
        {
            var drivesUsage = _databasesInfoRetriever.GetDrivesUsage();
            
            return new IoStatsPayload
            {
                Items = drivesUsage.Items.Select(x => x.IoStatsResult).Where(x => x != null).ToList()
            };
        }
    }
}
