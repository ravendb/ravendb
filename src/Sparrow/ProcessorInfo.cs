using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Voron.Platform.Posix;

namespace Sparrow
{
    public static class ProcessorInfo
    {
        private static Logger _logger = LoggingSource.Instance.GetLogger("ProcessorInfo", "Raven/Server");

        public static int ProcessorCount
        {
            get
            {
                if (PlatformDetails.RunningOnPosix == false)
                    return Environment.ProcessorCount;

                // get from cgroup (which is good for both container and non-container systems). use Environment.ProcessorCount only in case of failure to get from cgroup
                // need to support both cpuset.cpus, and cfs_quota (always only one is available, and we check them both at any case and taking the lowest)
                // https://docs.docker.com/engine/admin/resource_constraints/#configure-the-default-cfs-scheduler

                var coresList = SysUtils.ReadAndParseRangesFromFile("/sys/fs/cgroup/cpuset/cpuset.cpus");
                var quota = SysUtils.ReadULongFromFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
                var environmentProcessorCount = Environment.ProcessorCount;
                decimal coresViaCpus = environmentProcessorCount;
                decimal coresViaCpuset = environmentProcessorCount;
                if (quota <= long.MaxValue - 4 * 1024) // actuall != -1
                {
                    ulong period = SysUtils.ReadULongFromFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us"); // 100,000 is 100% per cpu
                    if (period != 0)
                        // ReSharper disable once PossibleLossOfFraction
                        coresViaCpus = Math.Round((decimal)(quota / period), 3, MidpointRounding.AwayFromZero);
                }
                if (coresList != null && coresList.Count > 0 && coresList.Count < environmentProcessorCount)
                    coresViaCpuset = coresList.Count;


                if (_logger.IsInfoEnabled)
                    _logger.Info($"ProccessorCount is the lowest between : Environment.ProcessorCount={Environment.ProcessorCount}, coresViaCpus={coresViaCpus}, coresViaCpuset={coresViaCpuset}");

                var numberOfCores = Math.Min(environmentProcessorCount, Math.Min(coresViaCpuset, coresViaCpus));
                // we are going to round down numberOfCores found, although container might server i.e. "2.5" cores
                return (int)numberOfCores;
            }
        }
    }
}
