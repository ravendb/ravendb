using System;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;

namespace Tests.Infrastructure
{
    public class TestResourcesAnalyzerMetricCacher : MetricCacher
    {
        private readonly SmapsReader _smapsReader;

        public TestResourcesAnalyzerMetricCacher(ICpuUsageCalculator cpuUsageCalculator)
        {
            if (PlatformDetails.RunningOnLinux)
                _smapsReader = new SmapsReader(new[] { new byte[SmapsReader.BufferSize], new byte[SmapsReader.BufferSize] });

            Register(MetricCacher.Keys.Server.CpuUsage, TimeSpan.FromSeconds(1), cpuUsageCalculator.Calculate);
            Register(MetricCacher.Keys.Server.MemoryInfo, TimeSpan.FromSeconds(1), CalculateMemoryInfo);
            Register(MetricCacher.Keys.Server.MemoryInfoExtended, TimeSpan.FromSeconds(1), CalculateMemoryInfoExtended);
        }

        private object CalculateMemoryInfo()
        {
            return MemoryInformation.GetMemoryInfo();
        }

        private object CalculateMemoryInfoExtended()
        {
            return MemoryInformation.GetMemoryInfo(_smapsReader, extended: true);
        }
    }
}
