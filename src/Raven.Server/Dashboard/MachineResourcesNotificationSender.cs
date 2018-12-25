using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.NotificationCenter;
using Raven.Server.Utils.Cpu;
using Sparrow;
using Sparrow.Collections;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Raven.Server.Dashboard
{
    public class MachineResourcesNotificationSender : BackgroundWorkBase
    {
        private readonly RavenServer _server;
        private readonly ConcurrentSet<ConnectedWatcher> _watchers;
        private readonly TimeSpan _notificationsThrottle;

        private DateTime _lastSentNotification = DateTime.MinValue;
        private readonly byte[][] _buffers;
        private readonly SmapsReader _smapsReader;

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

            if (PlatformDetails.RunningOnLinux)
            {
                var buffer1 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                var buffer2 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                _buffers = new[] { buffer1, buffer2 };
                _smapsReader = new SmapsReader(new[] { buffer1, buffer2 });
            }
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

                if (_watchers.Count == 0)
                    return;

                var machineResources = GetMachineResources(_smapsReader);
                foreach (var watcher in _watchers)
                {
                    // serialize to avoid race conditions
                    // please notice we call ToJson inside a loop since DynamicJsonValue is not thread-safe
                    watcher.NotificationsQueue.Enqueue(machineResources.ToJson());
                }
            }
            finally
            {
                _lastSentNotification = DateTime.UtcNow;
            }
        }

        public MachineResources GetMachineResources(SmapsReader smapsReader)
        {
            return GetMachineResources(smapsReader, _server.CpuUsageCalculator);
        }

        public static MachineResources GetMachineResources(SmapsReader smapsReader, ICpuUsageCalculator cpuUsageCalculator)
        {
            var memInfo = MemoryInformation.GetMemoryInfo(smapsReader, extendedInfo: true);
            var cpuInfo = cpuUsageCalculator.Calculate();

            var machineResources = new MachineResources
            {
                TotalMemory = memInfo.TotalPhysicalMemory.GetValue(SizeUnit.Bytes),
                AvailableMemory = memInfo.AvailableMemory.GetValue(SizeUnit.Bytes),
                AvailableWithoutTotalCleanMemory = memInfo.AvailableWithoutTotalCleanMemory.GetValue(SizeUnit.Bytes),
                SystemCommitLimit = memInfo.TotalCommittableMemory.GetValue(SizeUnit.Bytes),
                CommittedMemory = memInfo.CurrentCommitCharge.GetValue(SizeUnit.Bytes),
                ProcessMemoryUsage = memInfo.WorkingSet.GetValue(SizeUnit.Bytes),
                IsWindows = PlatformDetails.RunningOnPosix == false,
                IsLowMemory = LowMemoryNotification.Instance.IsLowMemory(memInfo, smapsReader, out var commitChargeThreshold),
                LowMemoryThreshold = LowMemoryNotification.Instance.LowMemoryThreshold.GetValue(SizeUnit.Bytes),
                CommitChargeThreshold = commitChargeThreshold.GetValue(SizeUnit.Bytes),
                MachineCpuUsage = cpuInfo.MachineCpuUsage,
                ProcessCpuUsage = cpuInfo.ProcessCpuUsage
            };

            return machineResources;
        }

        public new void Dispose()
        {
            base.Dispose();

            if (_buffers != null)
            {
                ArrayPool<byte>.Shared.Return(_buffers[0]);
                ArrayPool<byte>.Shared.Return(_buffers[1]);
            }
        }
    }
}
