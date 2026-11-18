using Tests.Infrastructure;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron;

public class PageHintsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const long PagesPerChunk = PageHints.ChunkLongs * 64L;

    [RavenFact(RavenTestCategory.Voron)]
    public void WithoutEvictionsAMarkStays()
    {
        var policy = new PrefetchPolicy(totalMemoryBytes: 0, startTimer: false);
        var hints = new PageHints(policy, PagesPerChunk * 4);
        Assert.True(hints.TryMark(0));

        for (var i = 0; i < 100; i++)
        {
            policy.AddEvictions(evictedFilePages: 0, filePages: 1000);
            policy.Tick(freeBytes: 0);
        }

        Assert.Equal(int.MaxValue, policy.StaleAfterSeconds);
        Assert.False(hints.TryMark(0));
        Assert.True(hints.IsMarked(0));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void TheStaleTimeIsHalfTheTurnoverOfTheLastTenSeconds()
    {
        var policy = new PrefetchPolicy(totalMemoryBytes: 0, startTimer: false);
        policy.AddEvictions(evictedFilePages: 100, filePages: 100_000);
        Assert.Equal(500, policy.StaleAfterSeconds); // 1,000 s to evict the whole cache

        for (var i = 0; i < 9; i++)
            policy.AddEvictions(evictedFilePages: 0, filePages: 100_000);
        Assert.Equal(5_000, policy.StaleAfterSeconds); // the same 100 pages over 10 seconds

        policy.AddEvictions(evictedFilePages: 0, filePages: 100_000);
        Assert.Equal(int.MaxValue, policy.StaleAfterSeconds);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AStaleChunkIsClearedOnItsNextUseAndOnlyIt()
    {
        var policy = new PrefetchPolicy(totalMemoryBytes: 0, startTimer: false);
        var hints = new PageHints(policy, PagesPerChunk * 3);
        long[] pages = [0, PagesPerChunk + 1, PagesPerChunk * 3 - 1];
        foreach (var page in pages)
            Assert.True(hints.TryMark(page));

        policy.AddEvictions(evictedFilePages: 1, filePages: 20); // stale after 10 s
        for (var i = 0; i < 9; i++)
            policy.Tick(freeBytes: 0);
        Assert.False(hints.TryMark(pages[0]));

        policy.Tick(freeBytes: 0); // 10 s
        Assert.True(hints.TryMark(pages[0]));
        Assert.True(hints.TryMark(pages[1]));
        Assert.False(hints.TryMark(pages[0])); // cleared at 10 s, marked again
        Assert.False(hints.TryMark(PagesPerChunk + 1));

        for (var i = 0; i < 5; i++)
            policy.Tick(freeBytes: 0);
        Assert.True(hints.TryMark(pages[2])); // a chunk not used in between: cleared on its first use
        Assert.False(hints.TryMark(pages[0])); // cleared 5 s ago
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void MarkReturnsTheFirstPageThatWasNotMarked()
    {
        var policy = new PrefetchPolicy(totalMemoryBytes: 0, startTimer: false);
        var hints = new PageHints(policy, PagesPerChunk * 2);
        Assert.True(hints.TryMark(PagesPerChunk - 2));
        Assert.True(hints.TryMark(PagesPerChunk - 1));

        Assert.Equal(PagesPerChunk, hints.Mark(PagesPerChunk - 2, 4)); // across two chunks
        Assert.Equal(-1, hints.Mark(PagesPerChunk - 2, 4));
        Assert.True(hints.IsMarked(PagesPerChunk + 1));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ThePagesOfAGrownFileAreTracked()
    {
        var policy = new PrefetchPolicy(totalMemoryBytes: 0, startTimer: false);
        var hints = new PageHints(policy, numberOfPages: 10);
        Assert.True(hints.TryMark(5));

        Assert.True(hints.TryMark(1_000_000));
        Assert.False(hints.TryMark(1_000_000));
        Assert.False(hints.TryMark(5));

        policy.AddEvictions(evictedFilePages: 1, filePages: 2); // stale after 1 s
        policy.Tick(freeBytes: 0);
        Assert.True(hints.TryMark(1_000_000));
        Assert.True(hints.TryMark(5));
    }
}
