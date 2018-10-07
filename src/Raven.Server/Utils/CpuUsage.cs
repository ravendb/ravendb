using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Raven.Client.Util;
using Raven.Server.Dashboard;
using Sparrow.Binary;
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
                (double MachineCpuUsage, double ProcessCpuUsage) cpuInfo;
                if (PlatformDetails.RunningOnPosix == false)
                {
                    cpuInfo = CalculateWindowsCpuUsage();
                }
                else if (PlatformDetails.RunningOnMacOsx)
                {
                    cpuInfo = CalculateMacOsCpuUsage();
                }
                else
                {
                    cpuInfo = CalculateLinuxCpuUsage();
                }

                _lastCpuInfo = cpuInfo;
                return _lastCpuInfo.Value;
            }
        }

        private static double CalculateProcessCpuUsage(ProcessInfo currentInfo, ProcessInfo previousInfo, double machineCpuUsage)
        {
            var processorTimeDiff = currentInfo.TotalProcessorTimeTicks - previousInfo.TotalProcessorTimeTicks;
            var timeDiff = currentInfo.TimeTicks - previousInfo.TimeTicks;
            if (timeDiff <= 0)
            {
                //overflow
                return _lastCpuInfo?.ProcessCpuUsage ?? 0;
            }

            if (currentInfo.ActiveCores == 0)
            {
                // shouldn't happen
                if (Logger.IsInfoEnabled)
                    Logger.Info($"ActiveCores == 0, OS: {RuntimeInformation.OSDescription}");

                return _lastCpuInfo?.ProcessCpuUsage ?? 0;
            }

            var processCpuUsage = (processorTimeDiff * 100.0) / timeDiff / currentInfo.ActiveCores;
            if (currentInfo.ActiveCores == ProcessorInfo.ProcessorCount)
            {
                // min as sometimes +-1% due to time sampling
                processCpuUsage = Math.Min(processCpuUsage, machineCpuUsage);
            }

            return Math.Min(100, processCpuUsage);
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
            if (sysTotal > 0)
            {
                machineCpuUsage = (sysTotal - systemIdleDiff) * 100.00 / sysTotal;
            }

            var processCpuUsage = CalculateProcessCpuUsage(windowsInfo, _previousWindowsInfo, machineCpuUsage);

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

            var processCpuUsage = CalculateProcessCpuUsage(linuxInfo, _previousLinuxInfo, machineCpuUsage);

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

            var processCpuUsage = CalculateProcessCpuUsage(macInfo, _previousMacInfo, machineCpuUsage);

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

            return new WindowsInfo
            {
                SystemIdleTime = GetTime(systemIdleTime),
                SystemKernelTime = GetTime(systemKernelTime),
                SystemUserTime = GetTime(systemUserTime)
            };
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

                return new LinuxInfo
                {
                    TotalUserTime = ulong.Parse(items[1]),
                    TotalUserLowTime = ulong.Parse(items[2]),
                    TotalSystemTime = ulong.Parse(items[3]),
                    TotalIdleTime = ulong.Parse(items[4]),
                    TotalIOTime = ulong.Parse(items[5]),
                    TotalIRQTime = ulong.Parse(items[6]),
                    TotalSoftIRQTime = ulong.Parse(items[7]),
                    TotalStealTime = ulong.Parse(items[8])
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

            return new MacInfo
            {
                TotalTicks = totalTicks,
                IdleTicks = hostCpuLoadInfo.cpu_ticks[(int)CpuState.CPU_STATE_IDLE]
            };
        }

        private static readonly unsafe int HostCpuLoadInfoSize = sizeof(host_cpu_load_info) / sizeof(uint);

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool GetSystemTimes(
            ref FileTime lpIdleTime,
            ref FileTime lpKernelTime,
            ref FileTime lpUserTime);

        public static long GetNumberOfActiveCores(Process process)
        {
            try
            {
                return Bits.NumberOfSetBits(process.ProcessorAffinity.ToInt64());
            }
            catch (NotSupportedException)
            {
                return ProcessorInfo.ProcessorCount;
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failure to get the number of active cores", e);

                return ProcessorInfo.ProcessorCount;
            }
        }

        public static (long TotalProcessorTimeTicks, long TimeTicks) GetProcessTimes(Process process)
        {
            try
            {
                var timeTicks = SystemTime.UtcNow.Ticks;
                var totalProcessorTime = process.TotalProcessorTime.Ticks;
                return (TotalProcessorTimeTicks: totalProcessorTime, TimeTicks: timeTicks);
            }
            catch (NotSupportedException)
            {
                return TryGetProcessTimesForLinux();
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failure to get process times, error: {e.Message}", e);

                return (0, 0);
            }
        }

        private static (long TotalProcessorTimeTicks, long TimeTicks) TryGetProcessTimesForLinux()
        {
            if (PlatformDetails.RunningOnLinux == false)
                return (0, 0);

            try
            {
                long timeTicks;
                long tmsStime;
                long tmsUtime;

                if (PlatformDetails.Is32Bits == false)
                {
                    var timeSample = new TimeSample();
                    timeTicks = Syscall.times(ref timeSample);
                    tmsStime = timeSample.tms_stime;
                    tmsUtime = timeSample.tms_utime;
                }
                else
                {
                    var timeSample = new TimeSample_32bit();
                    timeTicks = Syscall.times(ref timeSample);
                    tmsStime = timeSample.tms_stime;
                    tmsUtime = timeSample.tms_utime;
                }

                if (timeTicks == -1)
                {
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Got overflow time using the times system call " + Marshal.GetLastWin32Error());

                    return (0, 0);
                }

                return (TotalProcessorTimeTicks: tmsUtime + tmsStime, TimeTicks: timeTicks);
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failure to get process times for linux, error: {e.Message}", e);

                return (0, 0);
            }
        }

        private class ProcessInfo
        {
            protected ProcessInfo()
            {
                using (var process = Process.GetCurrentProcess())
                {
                    var processTimes = GetProcessTimes(process);
                    TotalProcessorTimeTicks = processTimes.TotalProcessorTimeTicks;
                    TimeTicks = processTimes.TimeTicks;

                    ActiveCores = GetNumberOfActiveCores(process);
                }
            }

            public long TotalProcessorTimeTicks { get; }

            public long TimeTicks { get; }

            public long ActiveCores { get; }
        }

        private class WindowsInfo : ProcessInfo
        {
            public ulong SystemIdleTime { get; set; }

            public ulong SystemKernelTime { get; set; }

            public ulong SystemUserTime { get; set; }
        }

        private class LinuxInfo : ProcessInfo
        {
            public ulong TotalUserTime { private get; set; }

            public ulong TotalUserLowTime { private get; set; }

            public ulong TotalSystemTime { private get; set; }

            public ulong TotalIdleTime { private get; set; }

            public ulong TotalIOTime { private get; set; }

            public ulong TotalIRQTime { private get; set; }

            public ulong TotalSoftIRQTime { private get; set; }

            public ulong TotalStealTime { private get; set; }

            public ulong TotalWorkTime => TotalUserTime + TotalUserLowTime + TotalSystemTime + 
                                          TotalIRQTime + TotalSoftIRQTime + TotalStealTime;

            public ulong TotalIdle => TotalIdleTime + TotalIOTime;
        }

        private class MacInfo : ProcessInfo
        {
            public ulong TotalTicks { get; set; }

            public ulong IdleTicks { get; set; }
        }
    }
}
