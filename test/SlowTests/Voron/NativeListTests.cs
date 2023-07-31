using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using FastTests.Voron;
using Voron;
using Voron.Util;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron;

public class NativeListTests : StorageTest
{
    public NativeListTests(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [InlineData(16)]
    [InlineData(1 << 10)]
    [InlineData(1 << 15)]
    public void CanAddWithGrowableAndOrderWithDataWillBePersisted(int size)
    {
        var random = new Random(12413123);
        var nativeList = new NativeList<long>();
        var nativeListScope = nativeList.Initialize(Allocator, size);
        var managedList = new List<long>();

        for (int idX = 0; idX < size; ++idX)
        {
            var initCapacity = nativeList.Capacity;
            var randomLong = random.NextInt64(long.MinValue, long.MaxValue);

            if (nativeList.TryPush(randomLong) == false )
                nativeList.Grow(Allocator, 1, ref nativeListScope);
            
            nativeList.PushUnsafe(randomLong);
            managedList.Add(randomLong);
            Assert.Equal(managedList.Count, nativeList.Count);

            //Assert whole list on extend
            if (initCapacity != nativeList.Capacity)
            {
                Assert.True(nativeList.ToSpan().SequenceEqual(CollectionsMarshal.AsSpan(managedList)));
            }
        }
        
        nativeList.Sort();
        managedList.Sort();
        Assert.True(CollectionsMarshal.AsSpan(managedList).SequenceEqual( nativeList.ToSpan()));

        var sizeBefore = nativeList.Capacity;
        nativeList.ResetAndEnsureCapacity(Allocator, size, ref nativeListScope);
        Assert.Equal(sizeBefore, nativeList.Capacity);
        Assert.Equal(0, nativeList.Count);
        
        nativeList.ResetAndEnsureCapacity(Allocator, (int)(size * 1.1), ref nativeListScope);
        Assert.NotEqual(sizeBefore, nativeList.Count);

        nativeListScope.Dispose();
    }
}
