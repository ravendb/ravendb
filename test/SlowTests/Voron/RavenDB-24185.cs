using System;
using FastTests;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron.Util;
using Xunit;
using Xunit.Abstractions;

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
        Assert.Equal("NativeList<System.Int64> cannot be larger than 268435452 items. Requested size: 268435456", ex.Message);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Memory)]
    public void ThrowWhenNativeListHasCapacityHigherThanPossibleOnGrow()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var nativeList = new NativeList<long>();
        nativeList.Initialize(allocator);

        var nextCapacity = (int.MaxValue / sizeof(long));
        var ex = Assert.Throws<InvalidOperationException>(() => nativeList.Grow(allocator, nextCapacity));
        Assert.Equal("NativeList<System.Int64> cannot be larger than 268435452 items. Requested size: 268435456", ex.Message);
    }
    
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Memory)]
    public void ThrowWhenNativeListHasCapacityHigherThanPossibleOnInitMaximum()
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var nativeList = new NativeList<long>();
        var ex = Assert.Throws<InvalidOperationException>(() => nativeList.Initialize(allocator, int.MaxValue / sizeof(long)));
        
        Assert.Equal($"NativeList<System.Int64> cannot be larger than 268435452 items. Requested size: {int.MaxValue / sizeof(long)}", ex.Message);
    }
}
