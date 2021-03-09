// -----------------------------------------------------------------------
//  <copyright file="CpuUsageWidget.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Utils;

namespace Raven.Server.ClusterDashboard.Widgets
{
    public class CpuUsageWidget : Widget
    {
        private readonly RavenServer _server;
        private readonly Action<CpuUsagePayload> _onMessage;

        private readonly TimeSpan _defaultInterval = TimeSpan.FromSeconds(1);
        private DetailsPerNode _nodeLicenseLimits;
        
        public CpuUsageWidget(int id, RavenServer server, Action<CpuUsagePayload> onMessage, CancellationToken shutdown) : base(id, shutdown)
        {
            _server = server;
            _onMessage = onMessage;
        }

        public override WidgetType Type => WidgetType.CpuUsage;

        protected override void InitializeWork()
        {
            //TODO: do we want to update that over time?
            _server.ServerStore.LicenseManager.GetCoresLimitForNode(out var licenseLimits);
            
            if (licenseLimits.NodeLicenseDetails.TryGetValue(_server.ServerStore.NodeTag, out var nodeLimits))
            {
                _nodeLicenseLimits = nodeLimits;
            }
        }

        protected override async Task DoWork()
        {
            var data = PrepareData();

            _onMessage(data);

            await WaitOrThrowOperationCanceled(_defaultInterval);
        }

        private CpuUsagePayload PrepareData()
        {
            var metricCacher = _server.MetricCacher;
            var cpuInfo = metricCacher.GetValue<(double MachineCpuUsage, double ProcessCpuUsage, double? MachineIoWait)>(
                MetricCacher.Keys.Server.CpuUsage);

            var utilizedCores = _nodeLicenseLimits?.UtilizedCores ?? -1;
            var numberOfCores = _nodeLicenseLimits?.NumberOfCores ?? -1;

            return new CpuUsagePayload
            {
                ProcessCpuUsage = (int)cpuInfo.ProcessCpuUsage,
                MachineCpuUsage = (int)cpuInfo.MachineCpuUsage,
                UtilizedCores = utilizedCores,
                NumberOfCores = numberOfCores,
                Time = SystemTime.UtcNow
            };
        }
    }
}
