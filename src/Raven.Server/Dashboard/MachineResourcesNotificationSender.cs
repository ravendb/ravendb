using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.NotificationCenter;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;
using Sparrow;
using Sparrow.Collections;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Server.LowMemory;

namespace Raven.Server.Dashboard
{
    public class MachineResourcesNotificationSender : BackgroundWorkBase
    {
        private readonly RavenServer _server;
        private readonly ConcurrentSet<ConnectedWatcher> _watchers;
        private readonly TimeSpan _notificationsThrottle;
        private readonly LowMemoryMonitor _lowMemoryMonitor;
        private DateTime _lastSentNotification = DateTime.MinValue;

        public MachineResourcesNotificationSender(
            string resourceName,
            RavenServer server,
            ConcurrentSet<ConnectedWatcher> watchers,
            TimeSpan notificationsThrottle,
            CancellationToken shutdown)
            : base(resourceName, shutdown)
        {
            _server = server;
            _watchers = watchers;
            _notificationsThrottle = notificationsThrottle;
            _lowMemoryMonitor = new LowMemoryMonitor();
        }

        protected override async Task DoWork()
        {
            var now = DateTime.UtcNow;
            var timeSpan = now - _lastSentNotification;
            if (timeSpan < _notificationsThrottle)
            {
                await WaitOrThrowOperationCanceled(_notificationsThrottle - timeSpan);
            }

            try
            {
                if (CancellationToken.IsCancellationRequested)
                    return;

                if (_watchers.IsEmpty)
                    return;

                var machineResources = GetMachineResources();
                foreach (var watcher in _watchers)
                {
                    // serialize to avoid race conditions
                    // please notice we call ToJson inside a loop since DynamicJsonValue is not thread-safe
                    watcher.Enqueue(machineResources.ToJson());
                }
            }
            finally
            {
                _lastSentNotification = DateTime.UtcNow;
            }
        }

        internal MachineResources GetMachineResources()
        {
            return GetMachineResources(_server.MetricCacher, _lowMemoryMonitor, _server.CpuUsageCalculator);
        }

        internal static MachineResources GetMachineResources(MetricCacher metricCacher, LowMemoryMonitor lowMemoryMonitor, ICpuUsageCalculator cpuUsageCalculator)
        {
            var memInfo = metricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfoExtended.RefreshRate15Seconds);
            var cpuInfo = metricCacher.GetValue(MetricCacher.Keys.Server.CpuUsage, cpuUsageCalculator.Calculate);

            var machineResources = new MachineResources
            {
                TotalMemory = memInfo.TotalPhysicalMemory.GetValue(SizeUnit.Bytes),
                AvailableMemory = memInfo.AvailableMemory.GetValue(SizeUnit.Bytes),
                AvailableMemoryForProcessing = memInfo.AvailableMemoryForProcessing.GetValue(SizeUnit.Bytes),
                SystemCommitLimit = memInfo.TotalCommittableMemory.GetValue(SizeUnit.Bytes),
                CommittedMemory = memInfo.CurrentCommitCharge.GetValue(SizeUnit.Bytes),
                ProcessMemoryUsage = memInfo.WorkingSet.GetValue(SizeUnit.Bytes),
                IsWindows = PlatformDetails.RunningOnPosix == false,
                LowMemorySeverity = LowMemoryNotification.Instance.IsLowMemory(memInfo, lowMemoryMonitor, out var commitChargeThreshold),
                LowMemoryThreshold = LowMemoryNotification.Instance.LowMemoryThreshold.GetValue(SizeUnit.Bytes),
                CommitChargeThreshold = commitChargeThreshold.GetValue(SizeUnit.Bytes),
                MachineCpuUsage = cpuInfo.MachineCpuUsage,
                ProcessCpuUsage = cpuInfo.ProcessCpuUsage,
                TotalSwapUsage = memInfo.TotalSwapUsage.GetValue(SizeUnit.Bytes)
            };

            return machineResources;
        }

        public override void Dispose()
        {
            base.Dispose();

            _lowMemoryMonitor?.Dispose();
        }
    }
}
