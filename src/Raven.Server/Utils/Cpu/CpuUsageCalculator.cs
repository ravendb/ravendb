using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Raven.Server.Logging;
using Raven.Server.Monitoring.Snmp.Objects.Server;
using Raven.Server.NotificationCenter;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Sparrow.Server.Platform.Posix.macOS;
using Sparrow.Utils;

namespace Raven.Server.Utils.Cpu
{
    public interface ICpuUsageCalculator : IDisposable
    {
        CpuUsageStats Calculate();
        
        void Init();
    }

    public sealed class CpuUsageStats
    {
        public static readonly CpuUsageStats EmptyCpuUsage = new(0.0, 0.0, (double?)null);
        public CpuUsageStats(double machineCpuUsage, double processCpuUsage, double? machineIoWait)
        {
            MachineCpuUsage = machineCpuUsage;
            ProcessCpuUsage = processCpuUsage;
            MachineIoWait = machineIoWait;
        }

        public double MachineCpuUsage;
        public double ProcessCpuUsage;
        public double? MachineIoWait;
    }
    
    internal abstract class CpuUsageCalculator<T> : ICpuUsageCalculator where T : ProcessInfo
    {
        protected readonly RavenLogger Logger;
        private readonly object _locker = new object();

        protected  CpuUsageStats LastCpuUsage;

        protected T PreviousInfo;

        protected CpuUsageCalculator()
        {
            Logger = RavenLogManager.Instance.GetLoggerForServer(GetType());
        }

        public void Init()
        {
            PreviousInfo = GetProcessInfo();
        }

        protected abstract (double MachineCpuUsage, double? MachineIoWait) CalculateMachineCpuUsage(T processInfo);

        public CpuUsageStats Calculate()
        {
            // this is a pretty quick method (sys call only), and shouldn't be
            // called heavily, so it is easier to make sure that this is thread
            // safe by just holding a lock.
            lock (_locker)
            {
                if (PreviousInfo == null)
                    return CpuUsageStats.EmptyCpuUsage;

                var currentInfo = GetProcessInfo();
                if (currentInfo == null)
                    return CpuUsageStats.EmptyCpuUsage;

                var machineCpuUsage = CalculateMachineCpuUsage(currentInfo);
                var processCpuUsage = CalculateProcessCpuUsage(currentInfo, machineCpuUsage.MachineCpuUsage);

                PreviousInfo = currentInfo;

                CpuUsageStats usage = new (machineCpuUsage.MachineCpuUsage, processCpuUsage, machineCpuUsage.MachineIoWait);
                LastCpuUsage = usage;
                return usage;
            }
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

        public void Dispose()
        {
        }
    }

    internal sealed class WindowsCpuUsageCalculator : CpuUsageCalculator<WindowsInfo>
    {
        protected override (double MachineCpuUsage, double? MachineIoWait) CalculateMachineCpuUsage(WindowsInfo windowsInfo)
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

            return (machineCpuUsage, null);
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

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool GetSystemTimes(
            ref FileTime lpIdleTime,
            ref FileTime lpKernelTime,
            ref FileTime lpUserTime);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ulong GetTime(FileTime fileTime)
        {
            return ((ulong)fileTime.dwHighDateTime << 32) | (uint)fileTime.dwLowDateTime;
        }

        [StructLayout(LayoutKind.Sequential)]
        internal struct FileTime
        {
            public int dwLowDateTime;
            public int dwHighDateTime;
        }
    }

    internal sealed class LinuxCpuUsageCalculator : CpuUsageCalculator<LinuxInfo>
    {
        private static readonly char[] Separators = { ' ', '\t' };

        protected override (double MachineCpuUsage, double? MachineIoWait) CalculateMachineCpuUsage(LinuxInfo linuxInfo)
        {
            double machineCpuUsage = 0;
            double? machineIoWait = 0;
            if (linuxInfo.TotalIdle >= PreviousInfo.TotalIdle &&
                linuxInfo.TotalWorkTime >= PreviousInfo.TotalWorkTime)
            {
                var idleDiff = linuxInfo.TotalIdle - PreviousInfo.TotalIdle;
                var workDiff = linuxInfo.TotalWorkTime - PreviousInfo.TotalWorkTime;
                var totalSystemWork = idleDiff + workDiff;
                var ioWaitDiff = linuxInfo.TotalIoWait - PreviousInfo.TotalIoWait;

                if (totalSystemWork > 0)
                {
                    machineCpuUsage = (workDiff * 100.0) / totalSystemWork;
                    machineIoWait = (ioWaitDiff * 100.0) / totalSystemWork;
                }
            }
            else if (LastCpuUsage != null)
            {
                // overflow
                machineCpuUsage = LastCpuUsage.MachineCpuUsage;
                machineIoWait = LastCpuUsage.MachineIoWait;
            }

            return (machineCpuUsage, machineIoWait);
        }

        protected override LinuxInfo GetProcessInfo()
        {
            var lines = File.ReadLines("/proc/stat");
            foreach (var line in lines)
            {
                if (line.StartsWith("cpu", StringComparison.OrdinalIgnoreCase) == false)
                    continue;

                var items = line.Split(Separators, StringSplitOptions.RemoveEmptyEntries);
                if (items.Length == 0 || items.Length < 9)
                    continue;

                return new LinuxInfo
                {
                    TotalUserTime = ulong.Parse(items[1]),
                    TotalUserLowTime = ulong.Parse(items[2]),
                    TotalSystemTime = ulong.Parse(items[3]),
                    TotalIdleTime = ulong.Parse(items[4]),
                    TotalIoWait = ulong.Parse(items[5]), 
                    TotalIRQTime = ulong.Parse(items[6]),
                    TotalSoftIRQTime = ulong.Parse(items[7]),
                    TotalStealTime = ulong.Parse(items[8])
                };
            }

            return null;
        }
    }

    internal sealed class MacInfoCpuUsageCalculator : CpuUsageCalculator<MacInfo>
    {
        private static readonly unsafe int HostCpuLoadInfoSize = sizeof(host_cpu_load_info) / sizeof(uint);

        protected override (double MachineCpuUsage, double? MachineIoWait) CalculateMachineCpuUsage(MacInfo macInfo)
        {
            var totalTicksSinceLastTime = macInfo.TotalTicks - PreviousInfo.TotalTicks;
            var idleTicksSinceLastTime = macInfo.IdleTicks - PreviousInfo.IdleTicks;
            double machineCpuUsage = 0;
            if (totalTicksSinceLastTime > 0)
            {
                machineCpuUsage = (1.0d - (double)idleTicksSinceLastTime / totalTicksSinceLastTime) * 100;
            }

            return (machineCpuUsage, null);
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

    internal sealed class ExtensionPointCpuUsageCalculator : ICpuUsageCalculator
    {
        private readonly CpuUsageExtensionPoint _inspector;

        public ExtensionPointCpuUsageCalculator(
            JsonContextPool contextPool,
            string exec,
            string args,
            ServerNotificationCenter notificationCenter)
        {
            _inspector = new CpuUsageExtensionPoint(
                contextPool,
                exec,
                args,
                notificationCenter
            );
        }

        public CpuUsageStats Calculate()
        {
            var data = _inspector.Data;
            return new (data.MachineCpuUsage, data.ProcessCpuUsage, null);
        }

        public void Init()
        {
            _inspector.Start();
        }

        public void Dispose()
        {
            _inspector.Dispose();
        }
    }
}
