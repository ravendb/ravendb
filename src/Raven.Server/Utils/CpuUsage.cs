using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Raven.Client.Util;
using Raven.Server.Config.Categories;
using Raven.Server.Dashboard;
using Sparrow.Binary;
using Sparrow.Json;
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
        private static ICpuUsageCalculator _calculator;

        public static CpuUsageExtensionPoint CpuUsageExtensionPoint { get; set; }

        static CpuUsage()
        {
            if (PlatformDetails.RunningOnPosix == false)
            {
                _calculator = new WindowsCpuUsageCalculator();
            }
            else if (PlatformDetails.RunningOnMacOsx)
            {
                _calculator = new MacInfoCpuUsageCalculator();
            }
            else
            {
                _calculator = new LinuxCpuUsageCalculator();
            }
            _calculator.Init();
        }

        public static (double MachineCpuUsage, double ProcessCpuUsage) Calculate()
        {
            // this is a pretty quick method (sys call only), and shouldn't be
            // called heavily, so it is easier to make sure that this is thread
            // safe by just holding a lock.
            lock (Locker)
            {
                return _calculator.Calculate();
            }
        }

        public static void UseCpuUsageExtensionPoint(
            JsonContextPool contextPool,
            MonitoringConfiguration configuration,
            NotificationCenter.NotificationCenter notificationCenter)
        {
            var extensionPoint = new ExtensionPointCpuUsageCalculator(
                contextPool,
                configuration.CpuUsageMonitorExec,
                configuration.CpuUsageMonitorExecArguments,
                Logger,
                notificationCenter);

            extensionPoint.Init();

            lock (Locker)
            {
                _calculator = extensionPoint;
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

            public double ActiveCores { get; }
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

        internal interface ICpuUsageCalculator
        {
            (double MachineCpuUsage, double ProcessCpuUsage) Calculate();
            void Init();
        }

        private abstract class CpuUsageCalculator<T> : ICpuUsageCalculator where T : ProcessInfo
        {
            // ReSharper disable once StaticMemberInGenericType
            private static readonly (double MachineCpuUsage, double ProcessCpuUsage) EmptyCpuUsage = (0, 0);

            protected (double MachineCpuUsage, double ProcessCpuUsage)? LastCpuUsage;

            protected T PreviousInfo;

            public void Init()
            {
                PreviousInfo = GetProcessInfo();
            }

            protected abstract double CalculateMachineCpuUsage(T processInfo);

            public (double MachineCpuUsage, double ProcessCpuUsage) Calculate()
            {
                if (PreviousInfo == null)
                    return EmptyCpuUsage;

                var currentInfo = GetProcessInfo();
                if (currentInfo == null)
                    return EmptyCpuUsage;

                var machineCpuUsage = CalculateMachineCpuUsage(currentInfo);
                var processCpuUsage = CalculateProcessCpuUsage(currentInfo, machineCpuUsage);

                PreviousInfo = currentInfo;

                LastCpuUsage = (machineCpuUsage, processCpuUsage);
                return LastCpuUsage.Value;
            }

            protected abstract T GetProcessInfo();

            private double CalculateProcessCpuUsage(ProcessInfo currentInfo, double machineCpuUsage)
            {
                var processorTimeDiff = currentInfo.TotalProcessorTimeTicks - PreviousInfo.TotalProcessorTimeTicks;
                var timeDiff = currentInfo.TimeTicks - PreviousInfo.TimeTicks;
                if (timeDiff <= 0)
                {
                    //overflow
                    return LastCpuUsage?.ProcessCpuUsage ?? 0;
                }

                if (currentInfo.ActiveCores <= 0)
                {
                    // shouldn't happen
                    if (Logger.IsInfoEnabled)
                    {
                        Logger.Info($"ProcessCpuUsage == {currentInfo.ActiveCores}, OS: {RuntimeInformation.OSDescription}");
                    }

                    return LastCpuUsage?.ProcessCpuUsage ?? 0;
                }

                var processCpuUsage = (processorTimeDiff * 100.0) / timeDiff / currentInfo.ActiveCores;
                if ((int)currentInfo.ActiveCores == ProcessorInfo.ProcessorCount)
                {
                    // min as sometimes +-1% due to time sampling
                    processCpuUsage = Math.Min(processCpuUsage, machineCpuUsage);
                }

                return Math.Min(100, processCpuUsage);
            }
        }

        private class WindowsCpuUsageCalculator : CpuUsageCalculator<WindowsInfo>
        {
            protected override double CalculateMachineCpuUsage(WindowsInfo windowsInfo)
            {
                var systemIdleDiff = windowsInfo.SystemIdleTime - PreviousInfo.SystemIdleTime;
                var systemKernelDiff = windowsInfo.SystemKernelTime - PreviousInfo.SystemKernelTime;
                var systemUserDiff = windowsInfo.SystemUserTime - PreviousInfo.SystemUserTime;
                var sysTotal = systemKernelDiff + systemUserDiff;

                double machineCpuUsage = 0;
                if (sysTotal > 0)
                {
                    machineCpuUsage = (sysTotal - systemIdleDiff) * 100.00 / sysTotal;
                }

                return machineCpuUsage;
            }

            protected override WindowsInfo GetProcessInfo()
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
        }

        private class LinuxCpuUsageCalculator : CpuUsageCalculator<LinuxInfo>
        {
            protected override double CalculateMachineCpuUsage(LinuxInfo linuxInfo)
            {
                double machineCpuUsage = 0;
                if (linuxInfo.TotalIdle >= PreviousInfo.TotalIdle &&
                    linuxInfo.TotalWorkTime >= PreviousInfo.TotalWorkTime)
                {
                    var idleDiff = linuxInfo.TotalIdle - PreviousInfo.TotalIdle;
                    var workDiff = linuxInfo.TotalWorkTime - PreviousInfo.TotalWorkTime;
                    var totalSystemWork = idleDiff + workDiff;

                    if (totalSystemWork > 0)
                    {
                        machineCpuUsage = (workDiff * 100.0) / totalSystemWork;
                    }
                }
                else if (LastCpuUsage != null)
                {
                    // overflow
                    machineCpuUsage = LastCpuUsage.Value.MachineCpuUsage;
                }

                return machineCpuUsage;
            }

            protected override LinuxInfo GetProcessInfo()
            {
                var lines = File.ReadLines("/proc/stat");
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
        }

        private class MacInfoCpuUsageCalculator : CpuUsageCalculator<MacInfo>
        {
            protected override double CalculateMachineCpuUsage(MacInfo macInfo)
            {
                var totalTicksSinceLastTime = macInfo.TotalTicks - PreviousInfo.TotalTicks;
                var idleTicksSinceLastTime = macInfo.IdleTicks - PreviousInfo.IdleTicks;
                double machineCpuUsage = 0;
                if (totalTicksSinceLastTime > 0)
                {
                    machineCpuUsage = (1.0d - (double)idleTicksSinceLastTime / totalTicksSinceLastTime) * 100;
                }

                return machineCpuUsage;
            }

            protected override unsafe MacInfo GetProcessInfo()
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
        }

        private class ExtensionPointCpuUsageCalculator : ICpuUsageCalculator
        {
            private readonly CpuUsageExtensionPoint _inspector;

            public ExtensionPointCpuUsageCalculator(
                JsonContextPool contextPool,
                string exec,
                string args,
                Logger logger,
                NotificationCenter.NotificationCenter notificationCenter)
            {
                _inspector = new CpuUsageExtensionPoint(
                    contextPool,
                    exec,
                    args,
                    logger,
                    notificationCenter
                );
            }

            public (double MachineCpuUsage, double ProcessCpuUsage) Calculate()
            {
                var data = _inspector.Data;
                return (data.MachineCpuUsage, data.ProcessCpuUsage);
            }

            public void Init()
            {
                _inspector.Start();
            }
        }
    }
}
