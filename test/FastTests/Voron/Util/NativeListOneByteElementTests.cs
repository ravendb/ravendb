using System;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron.Util;
using Xunit;

namespace FastTests.Voron.Util;

public unsafe class NativeListOneByteElementTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(1)]
    [InlineData(5)]
    [InlineData(4096)]
    public void AddRangeUnsafeAcceptsOneByteElements(int count)
    {
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        var source = new byte[count];
        for (var i = 0; i < count; i++)
            source[i] = (byte)(i + 1);

        var fromSpan = new NativeList<byte>();
        fromSpan.Initialize(allocator, count);
        fromSpan.AddRangeUnsafe(source);

        var fromPointer = new NativeList<byte>();
        fromPointer.Initialize(allocator, count);
        fixed (byte* p = source)
            fromPointer.AddRangeUnsafe(p, count);

        Assert.Equal(source, fromSpan.ToSpan().ToArray());
        Assert.Equal(source, fromPointer.ToSpan().ToArray());
    }
}
