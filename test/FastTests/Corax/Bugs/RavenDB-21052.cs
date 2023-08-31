using System;
using Corax.Queries.Meta;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_21052 : NoDisposalNeeded
{
    public RavenDB_21052(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanDetectGapBetweenArrays()
    {
        var right = new long[] {10, 11, 13, 15};
        var left = new long[] {8, 9};
        var dst = new long[2];

        //LEFT [RIGHT]
        //[8, 9] [10,..]
        var read = SortHelper.FindMatches(dst, left, right);
        Assert.Equal(0, read);

        left = new long[] {16, 17};

        //[RIGHT] [LEFT]
        //[10,..,15] [16,17]
        read = SortHelper.FindMatches(dst, left, right);
        Assert.Equal(0, read);
    }
    
    [Fact]
    public void CanPerformIntersectionWhenLastElementIsMarkedAsUsed()
    {
        var right = new long[] {10, 11, 13, 15};
        var left = new long[] {9, 15};
        var dst = new long[10];

        var read = SortHelper.FindMatches(dst.AsSpan(0, left.Length), left, right);
        Assert.Equal(1, read);
        left = new long[] {9, 11, 13};

        read = SortHelper.FindMatches(dst.AsSpan(0, left.Length), left, right);
        Assert.Equal(2, read);
    }
    
    [Fact]
    public void CanPerformIntersectionWhenFirstElementIsMarkedAsUsed()
    {
        var right = new long[] {10, 11, 13, 15};
        var left = new long[] {9, 10};
        var dst = new long[10];

        var read = SortHelper.FindMatches(dst.AsSpan(0, left.Length), left, right);
        Assert.Equal(1, read);
        left = new long[] {13, 15, 16}; // force order [right] [left]

        read = SortHelper.FindMatches(dst.AsSpan(0, left.Length), left, right);
        Assert.Equal(2, read);
        
    }
}
