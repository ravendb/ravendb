// -----------------------------------------------------------------------
//  <copyright file="CpuUsageWidget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using Raven.Server.Commercial;
using Raven.Server.NotificationCenter;
using Raven.Server.Utils;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class CpuUsageNotificationSender : AbstractClusterDashboardNotificationSender
    {
        private readonly RavenServer _server;

        private readonly TimeSpan _defaultInterval = TimeSpan.FromSeconds(1);
        private DetailsPerNode _nodeLicenseLimits;


        public CpuUsageNotificationSender(int widgetId, RavenServer server, ConnectedWatcher watcher, CancellationToken shutdown) : base(widgetId, watcher, shutdown)
        {
            _server = server;
        }

        protected override void InitializeWork()
        {
            //TODO: do we want to update that over time?
            _server.ServerStore.LicenseManager.GetCoresLimitForNode(out var licenseLimits);
            
            if (licenseLimits.NodeLicenseDetails.TryGetValue(_server.ServerStore.NodeTag, out var nodeLimits))
            {
                _nodeLicenseLimits = nodeLimits;
            }
        }

        protected override TimeSpan NotificationInterval => _defaultInterval;

        protected override AbstractClusterDashboardNotification CreateNotification()
        {
            var cpuInfo = _server.MetricCacher.GetValue<(double MachineCpuUsage, double ProcessCpuUsage, double? MachineIoWait)>(
                MetricCacher.Keys.Server.CpuUsage);

            var utilizedCores = _nodeLicenseLimits?.UtilizedCores ?? -1;
            var numberOfCores = _nodeLicenseLimits?.NumberOfCores ?? -1;

            return new CpuUsagePayload
            {
                ProcessCpuUsage = (int)cpuInfo.ProcessCpuUsage,
                MachineCpuUsage = (int)cpuInfo.MachineCpuUsage,
                UtilizedCores = utilizedCores,
                NumberOfCores = numberOfCores,
            };
        }
    }
}
