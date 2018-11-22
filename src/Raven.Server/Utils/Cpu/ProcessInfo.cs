using System.Diagnostics;

namespace Raven.Server.Utils.Cpu
{
    internal class ProcessInfo
    {
        protected ProcessInfo()
        {
            using (var process = Process.GetCurrentProcess())
            {
                var processTimes = CpuHelper.GetProcessTimes(process);
                TotalProcessorTimeTicks = processTimes.TotalProcessorTimeTicks;
                TimeTicks = processTimes.TimeTicks;

                ActiveCores = CpuHelper.GetNumberOfActiveCores(process);
            }
        }

        public long TotalProcessorTimeTicks { get; }

        public long TimeTicks { get; }

        public long ActiveCores { get; }
    }

    internal class WindowsInfo : ProcessInfo
    {
        public ulong SystemIdleTime { get; set; }

        public ulong SystemKernelTime { get; set; }

        public ulong SystemUserTime { get; set; }
    }

    internal class LinuxInfo : ProcessInfo
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

    internal class MacInfo : ProcessInfo
    {
        public ulong TotalTicks { get; set; }

        public ulong IdleTicks { get; set; }
    }
}
