// -----------------------------------------------------------------------
//  <copyright file="ClusterOverviewNotificationSender.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Xml;
using Raven.Client.Util;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class ClusterOverviewNotificationSender : AbstractClusterDashboardNotificationSender
    {
        private readonly RavenServer _server;
        // private readonly TimeSpan _defaultInterval = TimeSpan.FromMinutes(1);
        private readonly TimeSpan _defaultInterval = TimeSpan.FromSeconds(15); // todo - how often ?

        public ClusterOverviewNotificationSender(int widgetId, RavenServer server, ConnectedWatcher watcher, CancellationToken shutdown) : base(widgetId, watcher, shutdown)
        {
            _server = server;
        }

        protected override TimeSpan NotificationInterval => _defaultInterval;

        protected override AbstractClusterDashboardNotification CreateNotification()
        {
            var testUpTime = (SystemTime.UtcNow - _server.Statistics.StartUpTime).ToString("c");
            var testStartTime = _server.Statistics.StartUpTime;

            var serverStore = _server.ServerStore;
            var nodeTag = serverStore.NodeTag;

            var watchers = serverStore.GetClusterTopology().Watchers;
            var promotables =serverStore.GetClusterTopology().Promotables;

            var nodeType = "Member";
            if (watchers.ContainsKey(nodeTag))
            {
                nodeType = "Watcher";
            } else if (promotables.ContainsKey(nodeTag))
            {
                nodeType = "Promotable";
            }

            return new ClusterOverviewPayload
            {
                NodeTag = nodeTag,
                NodeUrl = _server.WebUrl,
                
                NodeType = nodeType,
                NodeState = _server.ServerStore.CurrentRachisState,
                
                StartTime = testStartTime
            };
        }
    }
}
