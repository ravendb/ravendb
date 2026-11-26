using System.Collections.Generic;
using Tests.Infrastructure;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron;

public class SequentialReadAheadTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const long FilePages = 1_000_000;

    [RavenFact(RavenTestCategory.Voron)]
    public void ARunIsReadAheadInWindowsThatDoubleUpTo256KB()
    {
        var readAhead = new SequentialReadAhead();
        var windows = new List<(long Start, int Pages, bool OnCallingThread, long ReadAt)>();
        for (long page = 100; page < 200; page++)
        {
            if (readAhead.TryAdvance(page, 1, FilePages, out var start, out var pages, out var onCallingThread))
                windows.Add((start, pages, onCallingThread, page));
        }

        Assert.Equal((102, SequentialReadAhead.FirstWindowPages, true, 101), windows[0]); // the second read starts the run
        Assert.Equal((106, 8, false, 103), windows[1]); // half of the window is left
        Assert.Equal((114, 16, false, 109), windows[2]);
        Assert.Equal((130, SequentialReadAhead.MaxWindowPages, false, 121), windows[3]);
        for (var i = 1; i < windows.Count; i++)
        {
            Assert.Equal(windows[i - 1].Start + windows[i - 1].Pages, windows[i].Start); // no page twice, no page skipped
            Assert.True(windows[i].Start > windows[i].ReadAt);
            Assert.True(windows[i].Pages <= SequentialReadAhead.MaxWindowPages);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ScatteredReadsAreNotReadAhead()
    {
        var readAhead = new SequentialReadAhead();
        foreach (var page in new long[] { 100, 5_000, 90_000, 77, 300_000, 120, 4_000 })
            Assert.False(readAhead.TryAdvance(page, 1, FilePages, out _, out _, out _));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void InterleavedRunsAreEachReadAhead()
    {
        var readAhead = new SequentialReadAhead();
        var starts = new HashSet<long>();
        for (var i = 0; i < 20; i++)
        {
            foreach (var run in new long[] { 1_000, 50_000, 900_000 })
            {
                if (readAhead.TryAdvance(run + i, 1, FilePages, out var start, out _, out _))
                    starts.Add(start - run);
            }
        }

        Assert.Equal(new HashSet<long> { 2, 6, 14 }, starts); // the same windows for each run
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AnOverflowValueAndSmallGapsContinueTheRun()
    {
        var readAhead = new SequentialReadAhead();
        Assert.False(readAhead.TryAdvance(100, 20, FilePages, out _, out _, out _)); // a value of 20 pages
        Assert.True(readAhead.TryAdvance(120 + SequentialReadAhead.MaxGapPages - 1, 1, FilePages, out var start, out _, out var onCallingThread));
        Assert.True(onCallingThread);
        Assert.Equal(120 + SequentialReadAhead.MaxGapPages, start);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void TheWindowEndsAtTheEndOfTheFile()
    {
        var readAhead = new SequentialReadAhead();
        Assert.False(readAhead.TryAdvance(96, 1, 100, out _, out _, out _));
        Assert.True(readAhead.TryAdvance(97, 1, 100, out var start, out var pages, out _));
        Assert.Equal(98, start);
        Assert.Equal(2, pages);
        Assert.False(readAhead.TryAdvance(98, 1, 100, out _, out _, out _));
        Assert.False(readAhead.TryAdvance(99, 1, 100, out _, out _, out _));
    }
}
