using System;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Sparrow.Utils
{
    public class ProcessorInfo
    {
        public readonly static int ProcessorCount = GetProcessorCount();

        public static int GetProcessorCount()
        {
            if (PlatformDetails.RunningOnPosix == false)
                return Environment.ProcessorCount;

            // on linux, if running in container, the number of cores might be set by either cpuset-cpus or cpus (cpus == cfs quota)
            // if not in container Environment.ProcessorCount is the value to be considered. So we read all the three values and return the lowest
            var cpussetCpusCores = CgroupUtils.ReadNumberOfCoresFromCgroupFile("/sys/fs/cgroup/cpuset/cpuset.cpus");
            var quota = CgroupUtils.ReadNumberFromCgroupFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
            var period = quota != long.MaxValue ? CgroupUtils.ReadNumberFromCgroupFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us") : 0; // read from file only if quota read successful
            var cpusCore = period != 0 ? quota / period : Environment.ProcessorCount; // note that we round down here ("2.5" processors quota will be read as "2")
            if (cpusCore == 0) // i.e. "0.5" processors quota
                cpusCore = 1;

            return Math.Min(Environment.ProcessorCount, Math.Min((int)cpusCore, cpussetCpusCores));
        }
    }
}
