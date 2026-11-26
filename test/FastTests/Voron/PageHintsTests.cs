using Tests.Infrastructure;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron;

public class PageHintsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const long PagesPerChunk = PageHints.ChunkLongs * 64L;
    private const long FilePages = 1 << 20;

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

        Assert.Equal(0, policy.PageCacheEpoch);
        Assert.False(hints.TryMark(0));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AMarkIsStaleAfterHalfThePageCacheIsEvictedHoweverTheEvictionsAreSpread()
    {
        var policy = new PrefetchPolicy(totalMemoryBytes: 0, startTimer: false);
        var hints = new PageHints(policy, PagesPerChunk);
        Assert.True(hints.TryMark(0));

        policy.AddEvictions(evictedFilePages: FilePages / 4, filePages: FilePages); // a burst
        for (var i = 0; i < 100; i++)
            policy.AddEvictions(evictedFilePages: 0, filePages: FilePages);
        policy.AddEvictions(evictedFilePages: FilePages / 4 - 1, filePages: FilePages);
        Assert.False(hints.TryMark(0));

        policy.AddEvictions(evictedFilePages: 1, filePages: FilePages);
        Assert.Equal(PrefetchPolicy.StaleAfterEpochs, policy.PageCacheEpoch);
        Assert.True(hints.TryMark(0));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AStaleChunkIsClearedOnItsNextUseAndOnlyIt()
    {
        var policy = new PrefetchPolicy(totalMemoryBytes: 0, startTimer: false);
        var hints = new PageHints(policy, PagesPerChunk * 3);
        long[] pages = [0, PagesPerChunk + 1, PagesPerChunk * 3 - 1];
        foreach (var page in pages)
            Assert.True(hints.TryMark(page));

        policy.AddEvictions(evictedFilePages: FilePages / 2 - 1, filePages: FilePages);
        Assert.False(hints.TryMark(pages[0]));

        policy.AddEvictions(evictedFilePages: 1, filePages: FilePages); // half the cache replaced
        Assert.True(hints.TryMark(pages[0]));
        Assert.True(hints.TryMark(pages[1]));
        Assert.False(hints.TryMark(pages[0])); // cleared, marked again
        Assert.False(hints.TryMark(PagesPerChunk + 1));

        policy.AddEvictions(evictedFilePages: FilePages / 4, filePages: FilePages);
        Assert.True(hints.TryMark(pages[2])); // a chunk not used in between: cleared on its first use
        Assert.False(hints.TryMark(pages[0])); // cleared 4 epochs ago
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
        Assert.False(hints.TryMark(PagesPerChunk + 1));
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

        policy.AddEvictions(evictedFilePages: 500, filePages: 1000); // half the page cache replaced
        Assert.True(hints.TryMark(1_000_000));
        Assert.True(hints.TryMark(5));
    }
}
