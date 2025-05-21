using System;
using System.Linq;
using FastTests.Voron.FixedSize;
using Sparrow.Binary;
using Tests.Infrastructure;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class RavenDB_24100(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void FindFirstFreeBitInBitmapTest()
    {
        const int bits = 256;
        ulong* buffer = stackalloc ulong[4];

        for (int bitIdx = 0; bitIdx < bits - 1; bitIdx++)
        {
            PtrBitVector.SetBitInPointer(buffer, bitIdx, true);
            var foundFreeBit = NewPageAllocator.TryFindFirstFreeBitInBitmap(buffer, out var result);
            var foundFreeBitManual = ManuallyCheck(buffer, out var positionManual);

            Assert.Equal(foundFreeBitManual, foundFreeBit);
            Assert.Equal(positionManual, result);
            Assert.Equal(bitIdx + 1, result);
        }

        PtrBitVector.SetBitInPointer(buffer, bits - 1, true);

        var foundFreeBitInFullBitmap = NewPageAllocator.TryFindFirstFreeBitInBitmap(buffer, out var resultInFullBitmap);
        var foundFreeBitInFullBitmapManual = ManuallyCheck(buffer, out var positionManualInFullBitmap);
        Assert.Equal(foundFreeBitInFullBitmapManual, foundFreeBitInFullBitmap);
        Assert.Equal(positionManualInFullBitmap, resultInFullBitmap);
        Assert.Equal(-1, resultInFullBitmap);
        Assert.False(foundFreeBitInFullBitmap);
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public unsafe void FindFirstFreeBitInBitmapTestFuzzy(int seed)
    {
        var random = new Random(seed);
        const int bits = 256;
        ulong* buffer = stackalloc ulong[4];
        var bufferAsSpan = new Span<ulong>(buffer, 4);
        for (int iteration = 0; iteration < 1000; ++iteration)
        {
            bufferAsSpan.Fill(ulong.MaxValue); // set as high
            var howManyToSetLow = random.Next(8, bits);
            var workingSet = GetListToSetLow(howManyToSetLow);
            foreach (var bitIdx in workingSet)
                PtrBitVector.SetBitInPointer(buffer, bitIdx, false);
            
            
            foreach (var bitIdx in workingSet)
            {
                var hasFreeSpace = NewPageAllocator.TryFindFirstFreeBitInBitmap(buffer, out var position);
                var hasFreeSpaceManual = ManuallyCheck(buffer, out var positionManualCheck);
                Assert.Equal(hasFreeSpaceManual, hasFreeSpace);
                Assert.Equal(positionManualCheck, position);
                PtrBitVector.SetBitInPointer(buffer, bitIdx, true);
            }
        }


        Span<int> GetListToSetLow(int count)
        {
            var result = Enumerable.Range(0, bits).ToArray().AsSpan();
            random.Shuffle(result);
            return result[..count];
        }
    }
    
    private static unsafe bool ManuallyCheck(ulong* buffer, out int position)
    {
        var bitsToCheck = 256;

        for (int bitIdx = 0; bitIdx < bitsToCheck; bitIdx++)
        {
            if (PtrBitVector.GetBitInPointer(buffer, bitIdx) == false)
            {
                position = bitIdx;
                return true;
            }
        }

        position = -1;
        return false;
    }
}
