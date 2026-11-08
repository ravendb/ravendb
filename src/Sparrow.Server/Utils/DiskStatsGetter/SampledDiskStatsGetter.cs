using System;
using System.Threading.Tasks;

namespace Sparrow.Server.Utils.DiskStatsGetter;

/// <summary>The disk stats of the device that holds a drive, from its <see cref="DeviceIoSampler"/>.</summary>
internal sealed class SampledDiskStatsGetter : IDiskStatsGetter
{
    public DiskStatsResult Get(string drive)
    {
        var stats = DeviceIoSampler.ForPath(drive)?.Current;
        if (stats == null)
            return null;

        return new DiskStatsResult
        {
            IoReadOperations = stats.ReadsPerSecond,
            IoWriteOperations = stats.WritesPerSecond,
            ReadThroughput = new Size((long)stats.ReadBytesPerSecond, SizeUnit.Bytes),
            WriteThroughput = new Size((long)stats.WriteBytesPerSecond, SizeUnit.Bytes),
            QueueLength = (long)Math.Round(stats.QueueDepth)
        };
    }

    public Task<DiskStatsResult> GetAsync(string drive) => Task.FromResult(Get(drive));

    public void Dispose()
    {
        // the samplers live for the lifetime of the process
    }
}
