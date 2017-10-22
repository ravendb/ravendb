using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.NotificationCenter;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Utils;

namespace Raven.Server.Dashboard
{
    public class MachineResourcesNotificationSender : BackgroundWorkBase
    {
        private new static readonly Logger Logger = LoggingSource.Instance.GetLogger<MachineResources>("Raven/Server");

        private readonly ConcurrentSet<ConnectedWatcher> _watchers;
        private readonly TimeSpan _notificationsThrottle;
        private static WindowsInfo _previousWindowsInfo;
        private static LinuxInfo _previousLinuxInfo;
        private static MacInfo _previousMacInfo;
        private static (double CpuUsage, double RavenCpuUsage)? _lastCpuInfo;
        private static readonly (double, double) EmptyCpuUsage = (0, 0);
        private DateTime _lastSentNotification = DateTime.MinValue;

        public MachineResourcesNotificationSender(string resourceName,
            ConcurrentSet<ConnectedWatcher> watchers, TimeSpan notificationsThrottle, CancellationToken shutdown)
            : base(resourceName, shutdown)
        {
            _watchers = watchers;
            _notificationsThrottle = notificationsThrottle;
            Initialize();
        }

        private static void Initialize()
        {
            if (PlatformDetails.RunningOnPosix == false)
            {
                _previousWindowsInfo = GetWindowsInfo();
                return;
            }

            if (PlatformDetails.RunningOnMacOsx)
            {
                _previousMacInfo = GetMacInfo();
                return;
            }

            _previousLinuxInfo = GetLinuxInfo();
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

        public static MachineResources GetMachineResources()
        {
            var currentProcess = Process.GetCurrentProcess();
            var workingSet =
                PlatformDetails.RunningOnPosix == false || PlatformDetails.RunningOnMacOsx
                    ? currentProcess.WorkingSet64
                    : MemoryInformation.GetRssMemoryUsage(currentProcess.Id);
            var memoryInfoResult = MemoryInformation.GetMemoryInfo();
            var installedMemory = memoryInfoResult.InstalledMemory.GetValue(SizeUnit.Bytes);
            var availableMemory = memoryInfoResult.AvailableMemory.GetValue(SizeUnit.Bytes);

            var cpuInfo = GetCpuInformation();
            _lastCpuInfo = cpuInfo;
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

        private static (double CpuUsage, double RavenCpuUsage) GetCpuInformation()
        {
            if (PlatformDetails.RunningOnPosix == false)
            {
                return CalculateWindowsCpuUsage();
            }

            if (PlatformDetails.RunningOnMacOsx)
            {
                return CalculateMacOsCpuUsage();
            }

            return CalculateLinuxCpuUsage();
        }

        private static (double CpuUsage, double RavenCpuUsage) CalculateWindowsCpuUsage()
        {
            if (_previousWindowsInfo == null)
                return EmptyCpuUsage;

            var windowsInfo = GetWindowsInfo();
            if (windowsInfo == null)
                return EmptyCpuUsage;

            var systemIdleDiff = windowsInfo.SystemIdleTime - _previousWindowsInfo.SystemIdleTime;
            var systemKernelDiff = windowsInfo.SystemKernelTime - _previousWindowsInfo.SystemKernelTime;
            var systemUserDiff = windowsInfo.SystemUserTime - _previousWindowsInfo.SystemUserTime;
            var sysTotal = systemKernelDiff + systemUserDiff;

            double cpuUsage = 0;
            double ravenCpuUsage = 0;
            if (sysTotal > 0)
            {
                cpuUsage = (sysTotal - systemIdleDiff) * 100.00 / sysTotal;

                var processTotal =
                    windowsInfo.ProcessProcessorTime.Ticks -
                    _previousWindowsInfo.ProcessProcessorTime.Ticks;
                ravenCpuUsage = (processTotal * 100.0) / sysTotal;
                ravenCpuUsage = Math.Min(100, ravenCpuUsage);
            }

            _previousWindowsInfo = windowsInfo;
            return (cpuUsage, ravenCpuUsage);
        }

        private static (double CpuUsage, double RavenCpuUsage) CalculateLinuxCpuUsage()
        {
            if (_previousLinuxInfo == null)
                return EmptyCpuUsage;

            var linuxInfo = GetLinuxInfo();
            if (linuxInfo == null)
                return EmptyCpuUsage;

            double cpuUsage = 0;
            if (linuxInfo.TotalUserTime >= _previousLinuxInfo.TotalUserTime &&
                linuxInfo.TotalUserLowTime >= _previousLinuxInfo.TotalUserLowTime &&
                linuxInfo.TotalSystemTime >= _previousLinuxInfo.TotalSystemTime &&
                linuxInfo.TotalIdleTime >= _previousLinuxInfo.TotalIdleTime)
            {
                var userTimeDiff = linuxInfo.TotalUserTime - _previousLinuxInfo.TotalUserTime;
                var userLowTimeDiff = linuxInfo.TotalUserLowTime - _previousLinuxInfo.TotalUserLowTime;
                var systemTimeDiff = linuxInfo.TotalSystemTime - _previousLinuxInfo.TotalSystemTime;
                var idleTimeDiff = linuxInfo.TotalIdleTime - _previousLinuxInfo.TotalIdleTime;

                var totalUsed = userTimeDiff + userLowTimeDiff + systemTimeDiff;
                cpuUsage = (totalUsed * 100.0) / (totalUsed + idleTimeDiff);
            }
            else if (_lastCpuInfo != null)
            {
                // overflow
                cpuUsage = _lastCpuInfo.Value.CpuUsage;
            }

            double ravenCpuUsage = 0;
            if (linuxInfo.Time > _previousLinuxInfo.Time &&
                linuxInfo.LastSystemCpu >= _previousLinuxInfo.LastSystemCpu &&
                linuxInfo.LastUserCpu >= _previousLinuxInfo.LastUserCpu)
            {
                var lastSystemCpuDiff = linuxInfo.LastSystemCpu - _previousLinuxInfo.LastSystemCpu;
                var lastUserCpuDiff = linuxInfo.LastUserCpu - _previousLinuxInfo.LastUserCpu;
                var totalCpuTime = lastSystemCpuDiff + lastUserCpuDiff;
                ravenCpuUsage = (totalCpuTime * 100.0 / (linuxInfo.Time - _previousLinuxInfo.Time)) / ProcessorInfo.ProcessorCount;
            }
            else if (_lastCpuInfo != null)
            {
                // overflow
                ravenCpuUsage = _lastCpuInfo.Value.RavenCpuUsage;
            }

            _previousLinuxInfo = linuxInfo;
            return (cpuUsage, ravenCpuUsage);
        }

        private static (double CpuUsage, double RavenCpuUsage) CalculateMacOsCpuUsage()
        {
            if (_previousMacInfo == null)
                return EmptyCpuUsage;

            var macInfo = GetMacInfo();
            if (macInfo == null)
                return EmptyCpuUsage;

            var totalTicksSinceLastTime = macInfo.TotalTicks - _previousMacInfo.TotalTicks;
            var idleTicksSinceLastTime = macInfo.IdleTicks - _previousMacInfo.IdleTicks;
            double cpuUsage = 0;
            if (totalTicksSinceLastTime > 0)
            {
                cpuUsage = (1.0d - (double)idleTicksSinceLastTime / totalTicksSinceLastTime) * 100;
            }

            var systemTimeDelta = macInfo.TaskTime - _previousMacInfo.TaskTime;
            var timeDelta = macInfo.DateTimeNanoTicks - _previousMacInfo.DateTimeNanoTicks;
            var ravenCpuUsage = 0d;
            if (timeDelta > 0)
            {
                ravenCpuUsage = (systemTimeDelta * 100.0) / timeDelta / ProcessorInfo.ProcessorCount;
            }

            _previousMacInfo = macInfo;

            return (cpuUsage, ravenCpuUsage);
        }

        private static WindowsInfo GetWindowsInfo()
        {
            var systemIdleTime = new FileTime();
            var systemKernelTime = new FileTime();
            var systemUserTime = new FileTime();
            if (GetSystemTimes(ref systemIdleTime, ref systemKernelTime, ref systemUserTime) == false)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failure when trying to get GetSystemTimes from Windows, error code was: " + Marshal.GetLastWin32Error());
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

        private static LinuxInfo GetLinuxInfo()
        {
            var lines = File.ReadAllLines("/proc/stat");
            foreach (var line in lines)
            {
                if (line.StartsWith("cpu", StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                var items = System.Text.RegularExpressions.Regex.Split(line, @"\s+").Where(s => s != string.Empty).ToArray();
                if (items.Length == 0)
                    continue;

                var timeSample = new TimeSample();

                var time = Syscall.times(ref timeSample);
                if (time == -1)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Got overflow time using the times system call " + Marshal.GetLastWin32Error());
                    return null;
                }

                return new LinuxInfo
                {
                    TotalUserTime = ulong.Parse(items[1]),
                    TotalUserLowTime = ulong.Parse(items[2]),
                    TotalSystemTime = ulong.Parse(items[3]),
                    TotalIdleTime = ulong.Parse(items[4]),
                    Time = time,
                    LastSystemCpu = timeSample.tms_stime,
                    LastUserCpu = timeSample.tms_utime,
                };
            }

            return null;
        }

        private static unsafe MacInfo GetMacInfo()
        {
            var machPort = Syscall.mach_host_self();
            var count = HostCpuLoadInfoSize;
            var hostCpuLoadInfo = new host_cpu_load_info();
            if (Syscall.host_statistics64(machPort, (int)FlavorMacOs.HOST_CPU_LOAD_INFO, &hostCpuLoadInfo, &count) != 0)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failure when trying to get hostCpuLoadInfo from MacOS, error code was: " + Marshal.GetLastWin32Error());
                return null;
            }

            ulong totalTicks = 0;
            for (var i = 0; i < (int)CpuState.CPU_STATE_MAX; i++)
                totalTicks += hostCpuLoadInfo.cpu_ticks[i];

            var size = ProcTaskAllInfoSize;
            var info = new proc_taskallinfo();
            var processId = Process.GetCurrentProcess().Id;
            var result = Syscall.proc_pidinfo(processId, (int)ProcessInfo.PROC_PIDTASKALLINFO, 0, &info, size);
            ulong dateTimeNanoTicks = 0;
            ulong userTime = 0;
            ulong systemTime = 0;
            if (result == size)
            {
                dateTimeNanoTicks = (ulong)DateTime.UtcNow.Ticks * 100;
                userTime = info.ptinfo.pti_total_user;
                systemTime = info.ptinfo.pti_total_system;
            }

            return new MacInfo
            {
                TotalTicks = totalTicks,
                IdleTicks = hostCpuLoadInfo.cpu_ticks[(int)CpuState.CPU_STATE_IDLE],
                DateTimeNanoTicks = dateTimeNanoTicks,
                TaskTime = systemTime + userTime
            };
        }

        private static readonly unsafe int ProcTaskAllInfoSize = sizeof(proc_taskallinfo);
        private static readonly unsafe int HostCpuLoadInfoSize = sizeof(host_cpu_load_info) / sizeof(uint);

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

        private class LinuxInfo
        {
            public ulong TotalUserTime { get; set; }

            public ulong TotalUserLowTime { get; set; }

            public ulong TotalSystemTime { get; set; }

            public ulong TotalIdleTime { get; set; }

            public long Time { get; set; }

            public long LastSystemCpu { get; set; }

            public long LastUserCpu { get; set; }
        }

        private class MacInfo
        {
            public ulong TotalTicks { get; set; }

            public ulong IdleTicks { get; set; }

            public ulong DateTimeNanoTicks { get; set; }

            public ulong TaskTime { get; set; }
        }
    }
}
