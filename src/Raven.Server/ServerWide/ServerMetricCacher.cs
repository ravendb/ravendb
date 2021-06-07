using System;
using Raven.Server.Utils;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Server.Utils;

namespace Raven.Server.ServerWide
{
    public class ServerMetricCacher : MetricCacher
    {
        private readonly SmapsReader _smapsReader;
        private readonly RavenServer _server;

        public ServerMetricCacher(RavenServer server)
        {
            _server = server;

            if (PlatformDetails.RunningOnLinux)
                _smapsReader = new SmapsReader(new[] { new byte[SmapsReader.BufferSize], new byte[SmapsReader.BufferSize] });
        }

        public void Initialize()
        {
            Register(MetricCacher.Keys.Server.CpuUsage, TimeSpan.FromSeconds(1), _server.CpuUsageCalculator.Calculate);
            Register(MetricCacher.Keys.Server.MemoryInfo, TimeSpan.FromSeconds(1), CalculateMemoryInfo);
            Register(MetricCacher.Keys.Server.MemoryInfoExtended, TimeSpan.FromSeconds(15), CalculateMemoryInfoExtended);
            Register(MetricCacher.Keys.Server.DiskSpaceInfo, TimeSpan.FromSeconds(15), CalculateDiskSpaceInfo);
            Register(MetricCacher.Keys.Server.GcAny, TimeSpan.FromSeconds(15), () => CalculateGcMemoryInfo(GCKind.Any));
            Register(MetricCacher.Keys.Server.GcBackground, TimeSpan.FromSeconds(15), () => CalculateGcMemoryInfo(GCKind.Background));
            Register(MetricCacher.Keys.Server.GcEphemeral, TimeSpan.FromSeconds(15), () => CalculateGcMemoryInfo(GCKind.Ephemeral));
            Register(MetricCacher.Keys.Server.GcFullBlocking, TimeSpan.FromSeconds(15), () => CalculateGcMemoryInfo(GCKind.FullBlocking));
        }

        private object CalculateMemoryInfo()
        {
            return MemoryInformation.GetMemoryInfo();
        }

        private object CalculateMemoryInfoExtended()
        {
            return MemoryInformation.GetMemoryInfo(_smapsReader, extended: true);
        }

        private DiskSpaceResult CalculateDiskSpaceInfo()
        {
            return DiskSpaceChecker.GetDiskSpaceInfo(_server.ServerStore.Configuration.Core.DataDirectory.FullPath);
        }

        private GCMemoryInfo CalculateGcMemoryInfo(GCKind gcKind)
        {
            return GC.GetGCMemoryInfo(gcKind);
        }
    }
}
