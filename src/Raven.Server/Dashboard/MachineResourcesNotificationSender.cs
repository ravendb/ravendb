using System;
using System.Buffers;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Extensions;
using Raven.Server.Background;
using Raven.Server.NotificationCenter;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Collections;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Raven.Server.Dashboard
{
    public class MachineResourcesNotificationSender : BackgroundWorkBase
    {
        private readonly ConcurrentSet<ConnectedWatcher> _watchers;
        private readonly TimeSpan _notificationsThrottle;

        private DateTime _lastSentNotification = DateTime.MinValue;

        public MachineResourcesNotificationSender(string resourceName,
            ConcurrentSet<ConnectedWatcher> watchers, TimeSpan notificationsThrottle, CancellationToken shutdown)
            : base(resourceName, shutdown)
        {
            _watchers = watchers;
            _notificationsThrottle = notificationsThrottle;
        }

        protected override async Task DoWork()
        {
            var now = DateTime.UtcNow;
            var timeSpan = now - _lastSentNotification;
            if (timeSpan < _notificationsThrottle)
            {
                await WaitOrThrowOperationCanceled(_notificationsThrottle - timeSpan);
            }

            byte[][] buffers = null;
            try
            {
                if (CancellationToken.IsCancellationRequested)
                    return;

                if (_watchers.Count == 0)
                    return;

                SmapsReader smapsReader = null;
                if (PlatformDetails.RunningOnLinux)
                {
                    var buffer1 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                    var buffer2 = ArrayPool<byte>.Shared.Rent(SmapsReader.BufferSize);
                    buffers = new []{buffer1, buffer2};
                    smapsReader = new SmapsReader(new[] {buffer1, buffer2});
                }
                var machineResources = GetMachineResources(smapsReader);
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
                if (buffers != null)
                {
                    ArrayPool<byte>.Shared.Return(buffers[0]);
                    ArrayPool<byte>.Shared.Return(buffers[1]);
                }
            }
        }

        

        public static MachineResources GetMachineResources(SmapsReader smapsReader)
        {
            using (var currentProcess = Process.GetCurrentProcess())
            {
                var memInfo = MemoryInformation.GetMemoryInfo(smapsReader);
                var isLowMemory = LowMemoryNotification.Instance.IsLowMemory(memInfo);
                var workingSet = PlatformDetails.RunningOnLinux
                    ? MemoryInformation.GetRssMemoryUsage(currentProcess.Id) - memInfo.AvailableWithoutTotalCleanMemory.GetValue(SizeUnit.Bytes)
                    : currentProcess.WorkingSet64;

                var cpuInfo = CpuUsage.Calculate();

                var machineResources = new MachineResources
                {
                    TotalMemory = memInfo.TotalPhysicalMemory.GetValue(SizeUnit.Bytes),
                    AvailableMemory = memInfo.AvailableMemory.GetValue(SizeUnit.Bytes),
                    AvailableWithoutTotalCleanMemory = memInfo.AvailableWithoutTotalCleanMemory.GetValue(SizeUnit.Bytes),
                    SystemCommitLimit = memInfo.TotalCommittableMemory.GetValue(SizeUnit.Bytes),
                    CommitedMemory = memInfo.CurrentCommitCharge.GetValue(SizeUnit.Bytes),
                    ProcessMemoryUsage = workingSet,
                    IsWindows = PlatformDetails.RunningOnPosix == false,
                    IsLowMemory = isLowMemory,
                    LowMemoryThreshold = LowMemoryNotification.Instance.LowMemoryThreshold.GetValue(SizeUnit.Bytes),
                    CommitChargeThreshold = LowMemoryNotification.Instance.GetCommitChargeThreshold(memInfo).GetValue(SizeUnit.Bytes),
                    MachineCpuUsage = cpuInfo.MachineCpuUsage,
                    ProcessCpuUsage = Math.Min(cpuInfo.MachineCpuUsage, cpuInfo.ProcessCpuUsage) // min as sometimes +-1% due to time sampling
                };

                return machineResources;
            }
        }
    }
}
