using System;
using Corax.Utils;
using FastTests;
using Sparrow.Server;
using Sparrow.Server.Collections;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Corax;

public class GrowableBitArrayTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenMultiplatformFact(RavenTestCategory.Corax, architecture: RavenArchitecture.AllX64)]
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
