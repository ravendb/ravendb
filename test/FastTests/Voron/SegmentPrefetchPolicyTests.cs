using System;
using System.Collections.Generic;
using System.Linq;
using Tests.Infrastructure;
using Sparrow.Server.Utils;
using Voron.Global;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron;

public class SegmentPrefetchPolicyTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const long SegmentSize = 4 * 1024 * 1024;
    private const int PagesPerSegment = (int)(SegmentSize / Constants.Storage.PageSize);
    private const long TotalMemory = 64L * 1024 * 1024 * 1024;
    private const long FreeMemory = TotalMemory / 2;
    private const long FullMemory = TotalMemory / 100;

    // a private policy driven by hand: no timer, no real memory
    private sealed class Harness : IDisposable
    {
        public readonly PrefetchPolicy Policy = new(TotalMemory, startTimer: false);
        public readonly SegmentPrefetchTable Table;
        public readonly List<long> Prefetches = new(); // segment numbers, in the order they were read ahead

        public Harness(long fileSegments, DeviceWriteBudget device = null)
        {
            Table = new SegmentPrefetchTable(Policy, device, SegmentSize, fileSegments * SegmentSize);
        }

        public void Memory(long freeBytes) => Policy.Tick(freeBytes);

        public void Access(IEnumerable<long> pages)
        {
            foreach (var page in pages)
            {
                if (Table.ShouldPrefetchSegment(page, out var offset, out var bytes))
                {
                    Assert.Equal(SegmentSize, bytes);
                    Prefetches.Add(offset / SegmentSize);
                }
            }
        }

        public void Dispose() => Policy.Dispose();
    }

    private static IEnumerable<long> RandomPages(Random random, long segments, int count)
    {
        for (var i = 0; i < count; i++)
            yield return random.NextInt64(segments * PagesPerSegment);
    }

    private static IEnumerable<long> FirstPageOf(int firstSegment, int segments) =>
        Enumerable.Range(firstSegment, segments).Select(x => (long)x * PagesPerSegment);

    [RavenFact(RavenTestCategory.Voron)]
    public void NothingIsReadAheadBeforeTheFirstMemorySample()
    {
        using var h = new Harness(fileSegments: 64);
        h.Access(RandomPages(new Random(1), 64, 1_000));
        Assert.Empty(h.Prefetches);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WhileMemoryIsFreeEveryTouchedSegmentIsReadAheadOnce()
    {
        using var h = new Harness(fileSegments: 640);
        h.Memory(FreeMemory);
        var pages = RandomPages(new Random(2), 640, 10_000).ToList();
        h.Access(pages);

        var touched = pages.Select(p => p / PagesPerSegment).Distinct().Order().ToList();
        Assert.Equal(touched, h.Prefetches.Order());
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WhenMemoryIsFullNothingIsReadAhead()
    {
        using var h = new Harness(fileSegments: 640);
        h.Memory(FreeMemory);
        h.Access(FirstPageOf(0, 10));
        Assert.Equal(10, h.Prefetches.Count);

        h.Memory(FullMemory);
        h.Prefetches.Clear();
        h.Access(RandomPages(new Random(3), 640, 10_000)); // uniform access over data larger than memory
        Assert.Empty(h.Prefetches);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ANewWarmUpStartsWhenMemoryIsFreeAgain()
    {
        using var h = new Harness(fileSegments: 640);
        h.Memory(FreeMemory);
        h.Access(FirstPageOf(0, 10));
        h.Access(FirstPageOf(0, 10)); // the same warm-up: already read ahead
        Assert.Equal(10, h.Prefetches.Count);

        h.Memory(FullMemory);
        h.Memory(FreeMemory); // large files were deleted, say: the segments may have been evicted since
        h.Prefetches.Clear();
        h.Access(FirstPageOf(0, 10));
        Assert.Equal(Enumerable.Range(0, 10).Select(x => (long)x), h.Prefetches);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void FreeMemoryBetweenTheStopAndStartLevelsKeepsTheCurrentState()
    {
        const long between = TotalMemory * (PrefetchPolicy.StopPercent + PrefetchPolicy.StartPercent) / 2 / 100;
        using var h = new Harness(fileSegments: 640);

        h.Memory(between); // not warming up yet: stays off
        h.Access(FirstPageOf(0, 10));
        Assert.Empty(h.Prefetches);

        h.Memory(FreeMemory);
        h.Memory(between); // warming up: stays on
        h.Access(FirstPageOf(0, 10));
        Assert.Equal(10, h.Prefetches.Count);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ManyWarmUpsNeverUseTheFullMemoryMarker()
    {
        using var h = new Harness(fileSegments: 1);
        for (var i = 0; i < 600; i++)
        {
            h.Memory(FreeMemory);
            Assert.NotEqual(0, h.Policy.Generation);
            h.Prefetches.Clear();
            h.Access(FirstPageOf(0, 1));
            Assert.Single(h.Prefetches);
            h.Memory(FullMemory);
            Assert.Equal(0, h.Policy.Generation);
        }
    }

    private sealed class FixedQueueDepth : DeviceQueueDepthReader
    {
        public override double Read() => 0;
        public override void Dispose() { }
    }

    // queue depth threshold 5, as Storage.SyncWritebackDrainQueueDepthThreshold
    private static DeviceWriteBudget Device() =>
        new(new FixedQueueDepth(), "test", syncCostThresholdTicks: long.MaxValue, queueDepthThreshold: 5, classifyAboveLatencyTicks: 0);

    [RavenFact(RavenTestCategory.Voron)]
    public void ReadAheadThatUsesItsWholeBudgetGetsTwiceAsMuchWhileTheQueueIsShort()
    {
        var device = Device();
        Assert.Equal(DeviceWriteBudget.MinReadAheadBytesPerInterval, device.ReadAheadBytesPerInterval);
        Assert.True(device.TryStartReadAhead(SegmentSize));
        Assert.False(device.TryStartReadAhead(SegmentSize));

        device.AdjustReadAhead(queueDepth: 1);
        Assert.Equal(2 * DeviceWriteBudget.MinReadAheadBytesPerInterval, device.ReadAheadBytesPerInterval);

        device.AdjustReadAhead(queueDepth: 1); // nothing was refused since: unchanged
        Assert.Equal(2 * DeviceWriteBudget.MinReadAheadBytesPerInterval, device.ReadAheadBytesPerInterval);

        for (var i = 0; i < 20; i++)
        {
            Assert.False(device.TryStartReadAhead(DeviceWriteBudget.MaxReadAheadBytesPerInterval + 1));
            device.AdjustReadAhead(queueDepth: 1);
        }
        Assert.Equal(DeviceWriteBudget.MaxReadAheadBytesPerInterval, device.ReadAheadBytesPerInterval);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ALongQueueHalvesTheReadAheadBudget()
    {
        var device = Device();
        for (var i = 0; i < 5; i++)
        {
            Assert.False(device.TryStartReadAhead(DeviceWriteBudget.MaxReadAheadBytesPerInterval + 1));
            device.AdjustReadAhead(queueDepth: 1);
        }
        Assert.Equal(32 * DeviceWriteBudget.MinReadAheadBytesPerInterval, device.ReadAheadBytesPerInterval);

        device.AdjustReadAhead(queueDepth: 6);
        Assert.Equal(16 * DeviceWriteBudget.MinReadAheadBytesPerInterval, device.ReadAheadBytesPerInterval);

        for (var i = 0; i < 10; i++)
            device.AdjustReadAhead(queueDepth: 50);
        Assert.Equal(DeviceWriteBudget.MinReadAheadBytesPerInterval, device.ReadAheadBytesPerInterval);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WithoutAQueueSignalReadAheadIsNotLimited()
    {
        var device = new DeviceWriteBudget(queueReader: null, "test", syncCostThresholdTicks: long.MaxValue, queueDepthThreshold: 5, classifyAboveLatencyTicks: 0);
        for (var i = 0; i < 1_000; i++)
            Assert.True(device.TryStartReadAhead(SegmentSize));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ASegmentTheBudgetRefusedIsReadAheadOnALaterTouch()
    {
        var device = Device();
        using var h = new Harness(fileSegments: 640, device);
        h.Memory(FreeMemory);

        h.Access(FirstPageOf(0, 10));
        Assert.Equal([0L], h.Prefetches); // the first interval's budget is one segment

        device.AdjustReadAhead(queueDepth: 1); // 2 segments
        h.Access(FirstPageOf(0, 10));
        Assert.Equal([0L, 1L, 2L], h.Prefetches);

        device.AdjustReadAhead(queueDepth: 1); // 4 segments
        h.Access(FirstPageOf(0, 10));
        device.AdjustReadAhead(queueDepth: 1); // 8 segments
        h.Access(FirstPageOf(0, 10));
        Assert.Equal(Enumerable.Range(0, 10).Select(x => (long)x), h.Prefetches);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void TheTableGrowsWithTheFile()
    {
        using var h = new Harness(fileSegments: 1);
        h.Memory(FreeMemory);
        h.Access(FirstPageOf(100, 1));
        Assert.Equal([100L], h.Prefetches);
    }
}
