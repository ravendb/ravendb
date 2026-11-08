using System.Diagnostics;
using System.IO;
using System.Threading;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Voron.Impl.Journal;
using Xunit;

namespace FastTests.Voron;

public class DeviceIoSamplerTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    // /sys/dev/block/<major>:<minor>/stat: reads, reads merged, sectors read, ms reading, writes, writes merged, sectors written,
    // ms writing, in flight, ms doing I/O, weighted ms doing I/O, then discard and flush fields
    private const string First = "     100        0     2000       50      200        0     4000      300        3      900     1200        0        0        0        0\n";
    private const string Second = "     200        0     4048      100      600        0    12192      500        1     2900     3700        0        0        0        0\n";

    [RavenMultiplatformFact(RavenTestCategory.Voron | RavenTestCategory.Monitoring, RavenPlatform.Linux)] // the sysfs reader; Windows also blocks rewriting the open file
    public void LinuxCountersGiveTheRatesOfTheInterval()
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, First);
            using var counters = DeviceCounters.LinuxSysfsCounters.TryCreate(file);
            Assert.NotNull(counters);

            var start = Stopwatch.GetTimestamp();
            Assert.False(counters.TrySample(start, out _)); // the first reading is the baseline

            File.WriteAllText(file, Second);
            Assert.True(counters.TrySample(start + 2 * Stopwatch.Frequency, out var stats));

            Assert.Equal(2000, stats.IntervalMs, precision: 6);
            Assert.Equal(50, stats.ReadsPerSecond, precision: 6); // 100 reads in 2 s
            Assert.Equal(200, stats.WritesPerSecond, precision: 6); // 400 writes in 2 s
            Assert.Equal(2048 * 512 / 2, stats.ReadBytesPerSecond, precision: 6);
            Assert.Equal(8192 * 512 / 2, stats.WriteBytesPerSecond, precision: 6);
            Assert.Equal(1.25, stats.QueueDepth, precision: 6); // 2500 weighted ms in 2000 ms
            Assert.Equal(0.5, stats.ReadLatencyMs, precision: 6); // 50 ms for 100 reads
            Assert.Equal(0.5, stats.WriteLatencyMs, precision: 6); // 200 ms for 400 writes
        }
        finally
        {
            File.Delete(file);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Voron | RavenTestCategory.Monitoring, RavenPlatform.Linux)] // the sysfs reader; Windows also blocks rewriting the open file
    public void LinuxCountersThatGoBackStartANewBaseline()
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, Second);
            using var counters = DeviceCounters.LinuxSysfsCounters.TryCreate(file);
            var start = Stopwatch.GetTimestamp();
            Assert.False(counters.TrySample(start, out _));

            File.WriteAllText(file, First); // the device was removed and added again
            Assert.False(counters.TrySample(start + Stopwatch.Frequency, out _));

            File.WriteAllText(file, Second);
            Assert.True(counters.TrySample(start + 2 * Stopwatch.Frequency, out var stats));
            Assert.Equal(100, stats.ReadsPerSecond, precision: 6);
        }
        finally
        {
            File.Delete(file);
        }
    }

    [RavenMultiplatformFact(RavenTestCategory.Voron | RavenTestCategory.Monitoring, RavenPlatform.Linux)] // the sysfs reader; Windows also blocks rewriting the open file
    public void LinuxCountersNeedTheWeightedTimeField()
    {
        var file = Path.GetTempFileName();
        try
        {
            File.WriteAllText(file, "    100     2000      200     4000\n"); // a partition on a very old kernel
            Assert.Null(DeviceCounters.LinuxSysfsCounters.TryCreate(file));
        }
        finally
        {
            File.Delete(file);
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Monitoring)]
    public void TheQueueDepthOfEachSampleDrivesTheDrainMode()
    {
        var counters = new FixedCounters();
        var sampler = new DeviceIoSampler(0, "test", counters);
        var device = new DeviceWriteBudget(sampler, "test", syncCostThresholdTicks: long.MaxValue, queueDepthThreshold: 5, classifyAboveLatencyTicks: 0);
        sampler.Sampled += device.OnSample;

        counters.QueueDepth = 2;
        for (var i = 0; i < 4; i++)
            sampler.Sample(Stopwatch.GetTimestamp());
        Assert.False(device.ShouldDrain());

        counters.QueueDepth = 9;
        for (var i = 0; i < 4; i++)
            sampler.Sample(Stopwatch.GetTimestamp());
        Assert.True(device.QueueDepth > 5, $"queue depth {device.QueueDepth}");
        Assert.True(device.ShouldDrain());
        Assert.Equal(9, sampler.Current.QueueDepth);

        // below the entry point but above the exit point (60%): stays in drain mode
        counters.QueueDepth = 4;
        for (var i = 0; i < 8; i++)
            sampler.Sample(Stopwatch.GetTimestamp());
        Assert.True(device.ShouldDrain());
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Monitoring)]
    public void ADeviceWhoseCountersFailedToOpenIsTriedAgainLater()
    {
        const ulong device = 0xFFFF_FFFF_FFFF_FF01; // no real device has this id
        var attempts = 0;
        DeviceIoSampler.OpenCountersForTesting = (id, _) =>
        {
            if (id != device)
                return null;
            if (Interlocked.Increment(ref attempts) == 1)
                throw new IOException("Too many open files");
            return new FixedCounters();
        };
        try
        {
            Assert.Null(DeviceIoSampler.ForDevice(device, "test"));
            Assert.Null(DeviceIoSampler.ForDevice(device, "test")); // too soon to try again
            Assert.Equal(1, attempts);

            DeviceIoSampler.RetryAt[device] = 0; // the retry interval passed
            var sampler = DeviceIoSampler.ForDevice(device, "test");
            Assert.NotNull(sampler);
            Assert.Same(sampler, DeviceIoSampler.ForDevice(device, "test"));
            Assert.Equal(2, attempts);
        }
        finally
        {
            DeviceIoSampler.OpenCountersForTesting = null;
        }
    }

    private sealed class FixedCounters : DeviceCounters
    {
        public double QueueDepth;

        public override bool TrySample(long timestamp, out DeviceIoStats stats)
        {
            stats = new DeviceIoStats { QueueDepth = QueueDepth, IntervalMs = 1000 };
            return true;
        }
    }
}
