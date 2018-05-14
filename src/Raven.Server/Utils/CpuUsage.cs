using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Raven.Server.Dashboard;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Platform.Posix.macOS;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    public static class CpuUsage
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<MachineResources>("Server");
        private static readonly object Locker = new object();
        private static WindowsInfo _previousWindowsInfo;
        private static LinuxInfo _previousLinuxInfo;
        private static MacInfo _previousMacInfo;
        private static (double MachineCpuUsage, double ProcessCpuUsage)? _lastCpuInfo;
        private static readonly (double MachineCpuUsage, double ProcessCpuUsage) EmptyCpuUsage = (0, 0);

        static CpuUsage()
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

        public static (double MachineCpuUsage, double ProcessCpuUsage) Calculate()
        {
            // this is a pretty quick method (sys call only), and shouldn't be
            // called heavily, so it is easier to make sure that this is thread
            // safe by just holding a lock.
            lock (Locker)
            {
                if (PlatformDetails.RunningOnPosix == false)
                {
                    var windowsCpuInfo = CalculateWindowsCpuUsage();
                    _lastCpuInfo = windowsCpuInfo;
                    return windowsCpuInfo;
                }

                if (PlatformDetails.RunningOnMacOsx)
                {
                    var macCpuInfo = CalculateMacOsCpuUsage();
                    _lastCpuInfo = macCpuInfo;
                    return macCpuInfo;
                }

                var linuxCpuInfo = CalculateLinuxCpuUsage();
                _lastCpuInfo = linuxCpuInfo;
                return linuxCpuInfo;
            }
        }

        private static (double MachineCpuUsage, double ProcessCpuUsage) CalculateWindowsCpuUsage()
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

            double machineCpuUsage = 0;
            double processCpuUsage = 0;
            if (sysTotal > 0)
            {
                machineCpuUsage = (sysTotal - systemIdleDiff) * 100.00 / sysTotal;

                var processTotal =
                    windowsInfo.ProcessProcessorTime.Ticks -
                    _previousWindowsInfo.ProcessProcessorTime.Ticks;
                processCpuUsage = (processTotal * 100.0) / sysTotal;
                processCpuUsage = Math.Min(100, processCpuUsage);
            }

            _previousWindowsInfo = windowsInfo;
            return (machineCpuUsage, processCpuUsage);
        }

        private static (double MachineCpuUsage, double ProcessCpuUsage) CalculateLinuxCpuUsage()
        {
            if (_previousLinuxInfo == null)
                return EmptyCpuUsage;

            var linuxInfo = GetLinuxInfo();
            if (linuxInfo == null)
                return EmptyCpuUsage;

            double machineCpuUsage = 0;
            if (linuxInfo.TotalIdle >= _previousLinuxInfo.TotalIdle &&
                linuxInfo.TotalWorkTime >= _previousLinuxInfo.TotalWorkTime)
            {
                var idleDiff = linuxInfo.TotalIdle - _previousLinuxInfo.TotalIdle;
                var workDiff = linuxInfo.TotalWorkTime - _previousLinuxInfo.TotalWorkTime;
                var totalSystemWork = idleDiff + workDiff;

                if (totalSystemWork > 0)
                {
                    machineCpuUsage = (workDiff * 100.0) / totalSystemWork;
                }
                else
                {
                    machineCpuUsage = 0;
                }
            }
            else if (_lastCpuInfo != null)
            {
                // overflow
                machineCpuUsage = _lastCpuInfo.Value.MachineCpuUsage;
            }

            double processCpuUsage = 0;
            if (linuxInfo.Time > _previousLinuxInfo.Time &&
                linuxInfo.LastSystemCpu >= _previousLinuxInfo.LastSystemCpu &&
                linuxInfo.LastUserCpu >= _previousLinuxInfo.LastUserCpu)
            {
                var lastSystemCpuDiff = linuxInfo.LastSystemCpu - _previousLinuxInfo.LastSystemCpu;
                var lastUserCpuDiff = linuxInfo.LastUserCpu - _previousLinuxInfo.LastUserCpu;
                var totalCpuTime = lastSystemCpuDiff + lastUserCpuDiff;
                processCpuUsage = (totalCpuTime * 100.0 / (linuxInfo.Time - _previousLinuxInfo.Time)) / ProcessorInfo.ProcessorCount;
            }
            else if (_lastCpuInfo != null)
            {
                // overflow
                processCpuUsage = _lastCpuInfo.Value.ProcessCpuUsage;
            }

            _previousLinuxInfo = linuxInfo;
            
            return (machineCpuUsage, processCpuUsage);
        }

        private static (double MachineCpuUsage, double ProcessCpuUsage) CalculateMacOsCpuUsage()
        {
            if (_previousMacInfo == null)
                return EmptyCpuUsage;

            var macInfo = GetMacInfo();
            if (macInfo == null)
                return EmptyCpuUsage;

            var totalTicksSinceLastTime = macInfo.TotalTicks - _previousMacInfo.TotalTicks;
            var idleTicksSinceLastTime = macInfo.IdleTicks - _previousMacInfo.IdleTicks;
            double machineCpuUsage = 0;
            if (totalTicksSinceLastTime > 0)
            {
                machineCpuUsage = (1.0d - (double)idleTicksSinceLastTime / totalTicksSinceLastTime) * 100;
            }

            var systemTimeDelta = macInfo.TaskTime - _previousMacInfo.TaskTime;
            var timeDelta = macInfo.DateTimeNanoTicks - _previousMacInfo.DateTimeNanoTicks;
            var processCpuUsage = 0d;
            if (timeDelta > 0)
            {
                processCpuUsage = (systemTimeDelta * 100.0) / timeDelta / ProcessorInfo.ProcessorCount;
            }

            _previousMacInfo = macInfo;

            return (machineCpuUsage, processCpuUsage);
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

            using (var process = Process.GetCurrentProcess())
            {
                var processTime = process.TotalProcessorTime;

                return new WindowsInfo
                {
                    SystemIdleTime = GetTime(systemIdleTime),
                    SystemKernelTime = GetTime(systemKernelTime),
                    SystemUserTime = GetTime(systemUserTime),
                    ProcessProcessorTime = processTime
                };
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong GetTime(FileTime fileTime)
        {
            return ((ulong)fileTime.dwHighDateTime << 32) | (uint)fileTime.dwLowDateTime;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct FileTime
        {
            public int dwLowDateTime;
            public int dwHighDateTime;
        }
        private static char[] _separators = new[] { ' ', '\t' };
        private static LinuxInfo GetLinuxInfo()
        {
            var lines = File.ReadAllLines("/proc/stat");
            foreach (var line in lines)
            {
                if (line.StartsWith("cpu", StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                var items = line.Split(_separators, StringSplitOptions.RemoveEmptyEntries);
                if (items.Length == 0 || items.Length < 9)
                    continue;

                long time = 0;
                long tmsStime = 0;
                long tmsUtime = 0;
                    
                if (PlatformDetails.Is32Bits == false)
                {
                    var timeSample = new TimeSample();
                    time = Syscall.times(ref timeSample);
                    tmsStime = timeSample.tms_stime;
                    tmsUtime = timeSample.tms_utime;
                }
                else
                {
                    var timeSample = new TimeSample_32bit();
                    time = Syscall.times(ref timeSample);
                    tmsStime = timeSample.tms_stime;
                    tmsUtime = timeSample.tms_utime;
                }
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
                    TotalIOTime = ulong.Parse(items[5]),
                    TotalIRQTime = ulong.Parse(items[6]),
                    TotalSoftIRQTime = ulong.Parse(items[7]),
                    TotalStealTime = ulong.Parse(items[8]),

                    Time = time,
                    LastSystemCpu = tmsStime,
                    LastUserCpu = tmsUtime,
                };
            }

            return null;
        }

        private static unsafe MacInfo GetMacInfo()
        {
            var machPort = macSyscall.mach_host_self();
            var count = HostCpuLoadInfoSize;
            var hostCpuLoadInfo = new host_cpu_load_info();
            if (macSyscall.host_statistics64(machPort, (int)Flavor.HOST_CPU_LOAD_INFO, &hostCpuLoadInfo, &count) != 0)
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
            
            var processId = 0;
            using (var currentProcess = Process.GetCurrentProcess())
                processId = currentProcess.Id;
            
            var result = macSyscall.proc_pidinfo(processId, (int)ProcessInfo.PROC_PIDTASKALLINFO, 0, &info, size);
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
        private static extern bool GetSystemTimes(
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

            public ulong TotalIOTime { get; internal set; }
            public ulong TotalIRQTime { get; internal set; }
            public ulong TotalSoftIRQTime { get; internal set; }
            public ulong TotalStealTime { get; internal set; }

            public ulong TotalWorkTime
            {
                get
                {
                    return TotalUserTime + TotalUserLowTime + TotalSystemTime + TotalIRQTime + TotalSoftIRQTime + TotalStealTime;
                }
            }

            public ulong TotalIdle
            {
                get { return TotalIdleTime + TotalIOTime; }
            }
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
