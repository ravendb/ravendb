using System;
using FastTests;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron.Util;
using Xunit;

namespace SlowTests.Voron;

public class RavenDB_24185(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Memory)]
    public void ThrowWhenNativeListHasCapacityHigherThanPossibleOnInit()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var nextCapacity = (int.MaxValue / sizeof(long)) + 1;
        var nativeList = new NativeList<long>();
        var ex = Assert.Throws<InvalidOperationException>(() => nativeList.Initialize(allocator, nextCapacity));
        Assert.Equal($"NativeList<System.Int64> cannot be larger than {NativeList<long>.MaxCapacity} items. Requested size: 268435456", ex.Message);

        // MaxCapacity takes into account the size of the ByteStringStorage. It contains a ptr, so the max capacity is dependent on the platform.
        var maxCapacity = Sparrow.Platform.PlatformDetails.Is32Bits ? 268435453 : 268435452;
        Assert.Equal(maxCapacity, NativeList<long>.MaxCapacity);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Memory)]
    public void ThrowWhenNativeListHasCapacityHigherThanPossibleOnGrow()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var nativeList = new NativeList<long>();
        nativeList.Initialize(allocator);

        var nextCapacity = (int.MaxValue / sizeof(long));
        var ex = Assert.Throws<InvalidOperationException>(() => nativeList.Grow(allocator, nextCapacity));
        Assert.Equal($"NativeList<System.Int64> cannot be larger than {NativeList<long>.MaxCapacity} items. Requested size: 268435456", ex.Message);
    }
    
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Memory)]
    public void ThrowWhenNativeListHasCapacityHigherThanPossibleOnInitMaximum()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var nativeList = new NativeList<long>();
        var ex = Assert.Throws<InvalidOperationException>(() => nativeList.Initialize(allocator, int.MaxValue / sizeof(long)));
        
        Assert.Equal($"NativeList<System.Int64> cannot be larger than {NativeList<long>.MaxCapacity} items. Requested size: {int.MaxValue / sizeof(long)}", ex.Message);
    }

    [RavenMultiplatformFact(category: RavenTestCategory.Voron | RavenTestCategory.Memory, platform: RavenPlatform.All, architecture: RavenArchitecture.AllX64)]
    public unsafe void EnsureThatNextBitsSizeIsNotLimitingCapacityDueToAligning()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var maxSize = (int.MaxValue - sizeof(ByteStringStorage)) / sizeof(long);
        var nativeList = new NativeList<long>();
        nativeList.Initialize(allocator, maxSize - 1);
    }
}
