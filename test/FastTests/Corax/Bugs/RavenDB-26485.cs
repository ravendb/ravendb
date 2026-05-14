using System;
using Corax.Querying.Matches.Meta;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax.Bugs;

public class RavenDB_26485(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Corax)]
    public void FindMatchesWillNotGoOutsideTheRanges()
    {
        const long different = 123;
        Span<long> dest = new long[2] { 1, 2 };
        Span<long> rightSourceBuffer = new long [5] { 1, 2, 3, 4, different };

        
        Span<long> left = new long [2] { 1, different };
        Span<long> right = rightSourceBuffer.Slice(0, 4); // we're using native memory inside corax
        Assert.Equal([1,2,3,4], right);
        
        var matches = SortHelper.FindMatches(dest, left, right);
        Assert.Equal(1, matches);
    }
}
