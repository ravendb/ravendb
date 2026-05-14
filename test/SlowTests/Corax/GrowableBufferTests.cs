using System;
using System.Numerics;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Corax;

public class GrowableBufferTests(ITestOutputHelper output) : NoDisposalNoOutputNeeded(output)
{
    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineData(4 * Sparrow.Global.Constants.Size.Megabyte)]
    [InlineData(8 * Sparrow.Global.Constants.Size.Megabyte)]
    public void CanExtendAndNotLooseAnythingSingle(int size) => CanExtendAndNotLooseAnythingBase<float>(size);
    
    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineData(4 * Sparrow.Global.Constants.Size.Megabyte)]
    [InlineData(8 * Sparrow.Global.Constants.Size.Megabyte)]
    public void CanExtendAndNotLooseAnything(int size) => CanExtendAndNotLooseAnythingBase<long>(size);
    
    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineData(16 * Sparrow.Global.Constants.Size.Megabyte)]
    [InlineData(32 * Sparrow.Global.Constants.Size.Megabyte)]
    public void CanExtendAndNotLooseAnythingExtended(int size) => CanExtendAndNotLooseAnythingBase<long>(size);
    
    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineData(16 * Sparrow.Global.Constants.Size.Megabyte)]
    [InlineData(32 * Sparrow.Global.Constants.Size.Megabyte)]
    public void CanExtendAndNotLooseAnythingExtendedSingle(int size) => CanExtendAndNotLooseAnythingBase<float>(size);
    
    private static void CanExtendAndNotLooseAnythingBase<T>(int size) where T : unmanaged, INumber<T>
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var growableBuffer = new GrowableBuffer<T, Progressive<T>>();
        growableBuffer.Init(bsc, 16);
        var count = 0;
        var random = new Random(15235);
        var random2 = new Random(15235);

        while (Fill(growableBuffer.GetSpace()) is var read and > 0)
        {
            growableBuffer.AddUsage(read);
        }

        Assert.Equal(size, growableBuffer.Results.Length);
        var results = growableBuffer.Results;
        for (var i = 0; i < size; ++i)
        {
            Assert.Equal(typeof(T) == typeof(long) ? (T)(object)random2.NextInt64() : (T)(object)(random2.NextSingle() * random2.Next()), results[i]);
        }
        
        int Fill(Span<T> buffer)
        {
            var i = 0;
            for (i = 0; i < buffer.Length && count < size; count++, i++)
                buffer[i] = typeof(T) == typeof(long) ? (T)(object)random.NextInt64() : (T)(object)(random.NextSingle() * random.Next());

            return i;
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void TryInitWithNoHintFallsBackToGrowthStrategyDefaultProgressive()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var buffer = new GrowableBuffer<long, Progressive<long>>();

        Assert.True(buffer.TryInit(bsc, initialSize: 0));

        // Progressive.GetInitialSize(0) yields 4 KB (4 * Constants.Size.Kilobyte).
        Assert.Equal(4 * Sparrow.Global.Constants.Size.Kilobyte, buffer.AllocatedSizeInBytes);
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void TryInitFailsWhenHintExceedsBudget()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var buffer = new GrowableBuffer<long, Progressive<long>>();

        Assert.False(buffer.TryInit(bsc, initialSize: 1000, maxAllocationInBytes: 512));
        Assert.False(buffer.IsInitialized);
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void TryInitFailsWhenStrategyDefaultExceedsBudget()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var buffer = new GrowableBuffer<long, Progressive<long>>();

        Assert.False(buffer.TryInit(bsc, initialSize: 0, maxAllocationInBytes: 64));
        Assert.False(buffer.IsInitialized);
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void InitThrowsWhenHintExceedsBudget()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var buffer = new GrowableBuffer<long, Progressive<long>>();

        Assert.Throws<InvalidOperationException>(() => buffer.Init(bsc, initialSize: 1000, maxAllocationInBytes: 512));
        Assert.False(buffer.IsInitialized);
    }
}
