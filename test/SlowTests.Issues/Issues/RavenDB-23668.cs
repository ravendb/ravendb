using System;
using System.Reflection;
using FastTests;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public unsafe class RavenDB_23668(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Core | RavenTestCategory.Memory)]
    public void ByteStringMemoryManagerAllocationTest()
    {
        const int arraySize = 65536;
        using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        using var scope = allocator.Allocate(arraySize, out Memory<long> memoryObject);
        Assert.Equal(memoryObject.Length, arraySize);

        var spanFromMemory = memoryObject.Span;
        Assert.Equal(spanFromMemory.Length, arraySize);
        
        var field = scope.GetType()
            .GetField("_str", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(field);

        var byteStringObj = field.GetValue(scope);
        Assert.NotNull(byteStringObj);
        Assert.IsType<ByteString>(byteStringObj);

        var bs = (ByteString)byteStringObj;
        using (var pinned = memoryObject.Pin())
        {
            var pinnedPointer = pinned.Pointer;
            var byteStringPointer = bs.Ptr;

            Assert.Equal((ulong)byteStringPointer, (ulong)pinnedPointer);
        }
    }
}
