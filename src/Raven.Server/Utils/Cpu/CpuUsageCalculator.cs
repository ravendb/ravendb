using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.InteropServices;
using Raven.Server.Dashboard;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Platform.Posix.macOS;
using Sparrow.Utils;

namespace Raven.Server.Utils.Cpu
{
    internal interface ICpuUsageCalculator
    {
        (double MachineCpuUsage, double ProcessCpuUsage) Calculate();
        void Init();
    }

    [SuppressMessage("ReSharper", "StaticMemberInGenericType")]
    internal abstract class CpuUsageCalculator<T> : ICpuUsageCalculator where T : ProcessInfo
    {
        protected static readonly Logger Logger = LoggingSource.Instance.GetLogger<MachineResources>("Server");
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

    internal class WindowsCpuUsageCalculator : CpuUsageCalculator<WindowsInfo>
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
            var systemIdleTime = new CpuUsage.FileTime();
            var systemKernelTime = new CpuUsage.FileTime();
            var systemUserTime = new CpuUsage.FileTime();
            if (CpuUsage.GetSystemTimes(ref systemIdleTime, ref systemKernelTime, ref systemUserTime) == false)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failure when trying to get GetSystemTimes from Windows, error code was: " + Marshal.GetLastWin32Error());
                return null;
            }

            return new WindowsInfo
            {
                SystemIdleTime = CpuUsage.GetTime(systemIdleTime),
                SystemKernelTime = CpuUsage.GetTime(systemKernelTime),
                SystemUserTime = CpuUsage.GetTime(systemUserTime)
            };
        }
    }

    internal class LinuxCpuUsageCalculator : CpuUsageCalculator<LinuxInfo>
    {
        private static char[] _separators = { ' ', '\t' };

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

    internal class MacInfoCpuUsageCalculator : CpuUsageCalculator<MacInfo>
    {
        private static readonly unsafe int HostCpuLoadInfoSize = sizeof(host_cpu_load_info) / sizeof(uint);

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

    internal class ExtensionPointCpuUsageCalculator : ICpuUsageCalculator
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
