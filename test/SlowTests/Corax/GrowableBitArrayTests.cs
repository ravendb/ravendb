using System;
using System.Collections.Generic;
using System.Linq;
using Corax.Utils;
using FastTests;
using FastTests.Voron.FixedSize;
using Sparrow.Server;
using Sparrow.Server.Collections;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;

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
    [InlineData((long)int.MaxValue)]
    [InlineData((long)int.MaxValue + 1)]
    public void CanStoreBigNumbers(long maxEntryId)
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using var lookup = new GrowableBitArray(allocator, maxEntryId);
        Assert.True(lookup.Add(maxEntryId));
        Assert.False(lookup.Add(maxEntryId));
    }


    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineData(1337, 100_000, 10_000)]
    [InlineData(2024, 1_000_000, 500)] // sparse - mostly zero words, exercises the vectorized skip
    [InlineData(42, 4_096, 4_096)]
    public void ReadsAllSetBitsInAscendingOrder(int seed, long capacity, int count)
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new GrowableBitArray(allocator, capacity);

        var random = new Random(seed);
        var ids = new HashSet<long>();
        while (ids.Count < count)
            ids.Add(random.NextInt64(1, capacity + 1));

        var idsArray = ids.OrderBy(x => x).ToArray();
        bitmap.AddRange(idsArray.AsSpan(0, idsArray.Length / 2));
        foreach (var id in idsArray.AsSpan(idsArray.Length / 2))
            bitmap.Add(id);

        var actual = new long[count + 16];
        var filled = bitmap.Fill(actual, 0);

        Assert.Equal(idsArray, actual.AsSpan(0, filled).ToArray());
    }

    [RavenMultiplatformFact(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    public void ResumeIsInclusiveAndPastTheEndIsEmpty()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        const long capacity = 1023;
        var bitmap = new GrowableBitArray(allocator, capacity);

        long[] ids = [1, 63, 64, 65, 127, 128, 500, 1022, 1023];
        bitmap.AddRange(ids);

        var buffer = new long[1];

        Assert.Equal(1, bitmap.Fill(buffer, 64));
        Assert.Equal(64, buffer[0]);

        Assert.Equal(1, bitmap.Fill(buffer, 65));
        Assert.Equal(65, buffer[0]);

        Assert.Equal(1, bitmap.Fill(buffer, capacity));
        Assert.Equal(capacity, buffer[0]);
        Assert.Equal(0, bitmap.Fill(buffer, capacity + 1));
    }

    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineData(3, 4_096)]
    [InlineData(7, 100_000)]
    [InlineData(511, 100_000)]
    public void FillDrainsInBatchesMatchingTheFullSet(int batchSize, long capacity)
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new GrowableBitArray(allocator, capacity);

        var random = new Random((int)capacity + batchSize);
        var ids = new HashSet<long>();
        // a dense consecutive run so small batches are forced to cut off mid-word...
        for (long id = 200; id < 330; id++)
            ids.Add(id);
        // ...plus random scatter
        while (ids.Count < capacity / 5)
            ids.Add(random.NextInt64(1, capacity + 1));
        bitmap.AddRange(ids.OrderBy(x => x).ToArray());

        // mirrors how FillViaBitmapOr streams the union across Fill calls
        var actual = new List<long>();
        var buffer = new long[batchSize];
        long lastReturned = 0;
        while (true)
        {
            int read = bitmap.Fill(buffer, lastReturned + 1);
            for (int i = 0; i < read; i++)
                actual.Add(buffer[i]);
            if (read < batchSize)
                break;

            lastReturned = actual[^1];
        }

        Assert.Equal(ids.OrderBy(x => x), actual);
    }

    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineData(3, 100_000, 20_000)]
    [InlineData(101, 100_000, 20_000)]
    [InlineData(4_096, 1_000_000, 500)]
    public void FillAndStreamsTheIntersectionInBatches(int batchSize, long capacity, int sideSize)
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var left = new GrowableBitArray(allocator, capacity);
        var right = new GrowableBitArray(allocator, capacity);

        var random = new Random(batchSize);
        var leftIds = new HashSet<long>();
        var rightIds = new HashSet<long>();
        for (long id = 400; id < 530; id++)
        {
            leftIds.Add(id);
            rightIds.Add(id);
        }

        while (leftIds.Count < sideSize)
            leftIds.Add(random.NextInt64(1, capacity + 1));
        while (rightIds.Count < sideSize)
            rightIds.Add(random.NextInt64(1, capacity + 1));

        left.AddRange(leftIds.OrderBy(x => x).ToArray());
        right.AddRange(rightIds.OrderBy(x => x).ToArray());

        var actual = new List<long>();
        var buffer = new long[batchSize];
        long lastReturned = 0;
        while (true)
        {
            int read = left.FillAnd(in right, buffer, lastReturned + 1);
            for (int i = 0; i < read; i++)
                actual.Add(buffer[i]);
            if (read < batchSize)
                break;

            lastReturned = actual[^1];
        }

        Assert.Equal(leftIds.Intersect(rightIds).OrderBy(x => x), actual);

        var leftAfter = new long[leftIds.Count + 1];
        Assert.Equal(leftIds.Count, left.Fill(leftAfter, 0));
        var rightAfter = new long[rightIds.Count + 1];
        Assert.Equal(rightIds.Count, right.Fill(rightAfter, 0));
    }

    [RavenMultiplatformFact(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    public void FillAndWithDisjointWindowsYieldsNothing()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var left = new GrowableBitArray(allocator, 100_000);
        var right = new GrowableBitArray(allocator, 100_000);

        left.AddRange(new long[] { 10, 20, 30 });
        right.AddRange(new long[] { 50_000, 60_000 });

        Assert.Equal(0, left.FillAnd(in right, new long[8], 0));
    }

    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineData(5, 100_000, 20_000)]
    [InlineData(77, 1_000_000, 1_000)]
    public void SubtractRemovesExactlyTheSetBits(int seed, long capacity, int excludedSize)
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var excluded = new GrowableBitArray(allocator, capacity);

        var random = new Random(seed);
        var excludedIds = new HashSet<long>();
        while (excludedIds.Count < excludedSize)
            excludedIds.Add(random.NextInt64(1, capacity + 1));
        excluded.AddRange(excludedIds.OrderBy(x => x).ToArray());

        var batch = new HashSet<long>(excludedIds.Take(500));
        for (long id = 1; id <= capacity; id += Math.Max(1, capacity / 4096))
            batch.Add(id);
        var work = batch.OrderBy(x => x).ToArray();
        var expected = work.Where(id => excludedIds.Contains(id) == false).ToArray();

        var kept = excluded.Subtract(work.AsSpan());

        Assert.Equal(expected, work.AsSpan(0, kept).ToArray());
    }

    [RavenMultiplatformFact(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    public void SubtractKeepsEverythingOutsideTheWindowAndOnEmptyBitmap()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var empty = new GrowableBitArray(allocator, 100_000);

        long[] batch = [1, 50, 99_999];
        Assert.Equal(3, empty.Subtract(batch.AsSpan()));
        Assert.Equal(new long[] { 1, 50, 99_999 }, batch);

        var excluded = new GrowableBitArray(allocator, 100_000);
        excluded.AddRange(new long[] { 5_000, 5_001, 5_002 });

        long[] mixed = [1, 4_999, 5_001, 5_003, 99_999];
        var kept = excluded.Subtract(mixed.AsSpan());
        Assert.Equal(new long[] { 1, 4_999, 5_003, 99_999 }, mixed.AsSpan(0, kept).ToArray());
    }

    [RavenMultiplatformFact(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    public void TracksTheSetBitWindow()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new GrowableBitArray(allocator, 100_000);

        Assert.True(bitmap.MinSetBit > bitmap.MaxSetBit); // empty
        Assert.Equal(0, bitmap.Fill(new long[4], 0));

        bitmap.AddRange(new long[] { 500, 501, 70_000 });
        bitmap.Add(499);
        Assert.Equal(499, bitmap.MinSetBit);
        Assert.Equal(70_000, bitmap.MaxSetBit);

        var contents = new long[8];
        Assert.Equal(4, bitmap.Fill(contents, 0));
        Assert.Equal(new long[] { 499, 500, 501, 70_000 }, contents.AsSpan(0, 4).ToArray());
    }
}
