using System;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using FastTests;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class GrowableBitArrayTests : NoDisposalNeeded
{
    public GrowableBitArrayTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanCreateEmptyBitmap()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using var lookup = new GrowableBitArray(allocator, 0);
    }

    public void CanCreateBitmapWithOneBitSet()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using var lookup = new GrowableBitArray(allocator, 1);
        Assert.True(lookup.Add(0));
        Assert.False(lookup.Add(0));
    }

    public void Allocates63BitsBoundary()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using var lookup = new GrowableBitArray(allocator, 63);
        Assert.True(lookup.Add(63));
        Assert.Throws<ArgumentOutOfRangeException>(() => lookup.Add(64));
    }
    
 
    
    [RavenFact(RavenTestCategory.Corax)]
    public void IsZeroed()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using var lookup = new GrowableBitArray(allocator, 128);
        for (int i = 1; i <= 128; ++i)
        {
            Assert.True(lookup.Add(i), i.ToString());
        }
    }

    [Fact]
    public void CanSetSecondBitmap()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using var lookup = new GrowableBitArray(allocator, GrowableBitArray.MaxCapacityPerBitmapInBits + 2);
        Assert.True(lookup.Add(GrowableBitArray.MaxCapacityPerBitmapInBits));
        Assert.False(lookup.Add(GrowableBitArray.MaxCapacityPerBitmapInBits));
        Assert.True(lookup.Add(GrowableBitArray.MaxCapacityPerBitmapInBits+1));
        Assert.False(lookup.Add(GrowableBitArray.MaxCapacityPerBitmapInBits+1));
        Assert.True(lookup.Add(GrowableBitArray.MaxCapacityPerBitmapInBits+2));
        Assert.False(lookup.Add(GrowableBitArray.MaxCapacityPerBitmapInBits+2));
        Assert.Throws<ArgumentOutOfRangeException>(() => lookup.Add(GrowableBitArray.MaxCapacityPerBitmapInBits+3));
    }
}
