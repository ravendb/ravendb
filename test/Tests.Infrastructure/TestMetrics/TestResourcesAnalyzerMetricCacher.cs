using System;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Tests.Infrastructure.TestMetrics
{
    public class TestResourcesAnalyzerMetricCacher : MetricCacher
    {
        private readonly SmapsReader _smapsReader;
        private readonly TimeSpan _cacheRefreshRate = TimeSpan.FromMilliseconds(25);

        public TestResourcesAnalyzerMetricCacher(ICpuUsageCalculator cpuUsageCalculator)
        {
            if (PlatformDetails.RunningOnLinux)
                _smapsReader = new SmapsReader(new[] { new byte[SmapsReader.BufferSize], new byte[SmapsReader.BufferSize] });

            Register(Keys.Server.CpuUsage, _cacheRefreshRate, cpuUsageCalculator.Calculate);
            Register(Keys.Server.MemoryInfo, _cacheRefreshRate, CalculateMemoryInfo);
            Register(Keys.Server.MemoryInfoExtended, _cacheRefreshRate, CalculateMemoryInfoExtended);
        }

        private static object CalculateMemoryInfo()
            => MemoryInformation.GetMemoryInfo();

        private object CalculateMemoryInfoExtended()
            => MemoryInformation.GetMemoryInfo(_smapsReader, extended: true);

        public (double MachineCpuUsage, double ProcessCpuUsage, double? MachineIoWait) GetCpuUsage()
            => GetValue<(double MachineCpuUsage, double ProcessCpuUsage, double? MachineIoWait)>(Keys.Server.CpuUsage);

        public MemoryInfoResult GetMemoryInfoExtended()
            => GetValue<MemoryInfoResult>(Keys.Server.MemoryInfoExtended);
    }
}
