using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.NotificationCenter;
using Sparrow;
using Sparrow.Collections;
using Sparrow.LowMemory;
using Sparrow.Platform;

namespace Raven.Server.Dashboard
{
    public class MachineResourcesNotificationSender : BackgroundWorkBase
    {
        private readonly ConcurrentSet<ConnectedWatcher> _watchers;
        private readonly TimeSpan _notificationsThrottle;
        private WindowsInfo _windowsInfo;
        private DateTime _lastSentNotification = DateTime.MinValue;

        public MachineResourcesNotificationSender(string resourceName,
            ConcurrentSet<ConnectedWatcher> watchers, TimeSpan notificationsThrottle, CancellationToken shutdown)
            : base(resourceName, shutdown)
        {
            _watchers = watchers;
            _notificationsThrottle = notificationsThrottle;
            Initialize();
        }

        private static WindowsInfo GetWindowsInfo()
        {
            var systemIdleTime = new FileTime();
            var systemKernelTime = new FileTime();
            var systemUserTime = new FileTime();
            if (GetSystemTimes(ref systemIdleTime, ref systemKernelTime, ref systemUserTime) == false)
            {
                //TODO: log that we failed to get system times
                return null;
            }

            var process = Process.GetCurrentProcess();
            var processTime = process.TotalProcessorTime;

            return new WindowsInfo
            {
                SystemIdleTime = GetTime(systemIdleTime),
                SystemKernelTime = GetTime(systemKernelTime),
                SystemUserTime = GetTime(systemUserTime),
                ProcessProcessorTime = processTime
            };
        }

        private void Initialize()
        {
            if (PlatformDetails.RunningOnPosix == false)
            {
                var windowsInfo = GetWindowsInfo();
                if (windowsInfo == null)
                    return;

                _windowsInfo = windowsInfo;
                return;
            }

            //TODO: calculate for linux and mac
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

                var machineResources = GetMachineResources();
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

        private MachineResources GetMachineResources()
        {
            var currentProcess = Process.GetCurrentProcess();
            var workingSet = PlatformDetails.RunningOnPosix == false ? 
                currentProcess.WorkingSet64 : 
                MemoryInformation.GetRssMemoryUsage(currentProcess.Id);

            var memoryInfoResult = MemoryInformation.GetMemoryInfo();
            var installedMemory = memoryInfoResult.InstalledMemory.GetValue(SizeUnit.Bytes);
            var availableMemory = memoryInfoResult.AvailableMemory.GetValue(SizeUnit.Bytes);

            var cpuInfo = GetCpuInformation();
            var machineResources = new MachineResources
            {
                TotalMemory = installedMemory,
                MemoryUsage = installedMemory - availableMemory,
                RavenMemoryUsage = workingSet,
                CpuUsage = cpuInfo.CpuUsage,
                RavenCpuUsage = cpuInfo.RavenCpuUsage
            };

            return machineResources;
        }

        private (double CpuUsage, double RavenCpuUsage) GetCpuInformation()
        {
            if (PlatformDetails.RunningOnPosix == false)
            {
                if (_windowsInfo == null)
                    return (-1, -1);

                var windowsInfo = GetWindowsInfo();
                if (windowsInfo == null)
                    return (-1, -1);

                var systemIdleDiff = windowsInfo.SystemIdleTime - _windowsInfo.SystemIdleTime;
                var systemKernelDiff = windowsInfo.SystemKernelTime - _windowsInfo.SystemKernelTime;
                var systemUserDiff = windowsInfo.SystemUserTime - _windowsInfo.SystemUserTime;
                var sysTotal = systemKernelDiff + systemUserDiff;

                double cpuUsage = 0;
                double ravenCpuUsage = 0;
                if (sysTotal > 0)
                {
                    cpuUsage = (sysTotal - systemIdleDiff) * 100.00 / sysTotal;

                    var processTotal = 
                        windowsInfo.ProcessProcessorTime.Ticks - 
                        _windowsInfo.ProcessProcessorTime.Ticks;
                    ravenCpuUsage = (processTotal * 100.0) / sysTotal;
                    ravenCpuUsage = Math.Min(100, ravenCpuUsage);
                }

                _windowsInfo = windowsInfo;
                return (cpuUsage, ravenCpuUsage);
            }

            //TODO: calculate for linux and mac
            return (-1, -1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong GetTime(FileTime fileTime)
        {
            return ((ulong)fileTime.dwHighDateTime << 32) | (uint)fileTime.dwLowDateTime;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct FileTime
        {
            public int dwLowDateTime;
            public int dwHighDateTime;
        }

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool GetSystemTimes(
            ref FileTime lpIdleTime,
            ref FileTime lpKernelTime,
            ref FileTime lpUserTime);

        private class WindowsInfo
        {
            public ulong SystemIdleTime { get; set; }

            public ulong SystemKernelTime { get; set; }

            public ulong SystemUserTime { get; set; }

            public TimeSpan ProcessProcessorTime { get; set; }
        }
    }
}
