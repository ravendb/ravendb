using System;
using System.Collections.Generic;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using FastTests;
using FastTests.Voron.FixedSize;
using Sparrow.Server;
using Sparrow.Server.Collections;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class GrowableBitArrayTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Corax)]
    public void CanCreateEmptyBitmap()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using var lookup = new GrowableBitArray(allocator, 0);
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanCreateBitmapWithOneBitSet()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using var lookup = new GrowableBitArray(allocator, 1);
        Assert.True(lookup.Add(0));
        Assert.False(lookup.Add(0));
    }

    [RavenFact(RavenTestCategory.Corax)]
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
    
    [RavenTheory(RavenTestCategory.Corax)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public void FuzzyTestOfGrowableBitArray(int seed)
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var random = new Random(seed);
        var count = random.Next(1, 7_500_000);
        var operations = random.Next(1, 10_000_000);
        using var lookup = new GrowableBitArray(allocator, count);
        HashSet<long> marked = new();
        for (int i = 0; i < operations; ++i)
        {
            var idX = random.Next(1, count + 1);
            var expectedAnswer = marked.Add(idX);
            var actualAnswer = lookup.Add(idX);
            Assert.Equal(expectedAnswer, actualAnswer);
        }
    }
    
    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineData((long)int.MaxValue - 1)]
    [InlineData((long)int.MaxValue )]
    [InlineData((long)int.MaxValue + 1)]
    public void CanStoreBigNumbers(long maxEntryId)
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using var lookup = new GrowableBitArray(allocator, maxEntryId);
        Assert.True(lookup.Add(maxEntryId));
        Assert.False(lookup.Add(maxEntryId));
    }
}
