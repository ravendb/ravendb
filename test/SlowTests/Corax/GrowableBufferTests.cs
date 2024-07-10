using System;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class GrowableBufferTests : NoDisposalNoOutputNeeded
{
    public GrowableBufferTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.All)]
    [InlineData(4 * Sparrow.Global.Constants.Size.Megabyte)]
    [InlineData(8 * Sparrow.Global.Constants.Size.Megabyte)]
    public void CanExtendAndNotLooseAnything(int size) => CanExtendAndNotLooseAnythingBase(size);
    
    [RavenMultiplatformTheory(RavenTestCategory.Corax, RavenArchitecture.AllX64)]
    [InlineData(16 * Sparrow.Global.Constants.Size.Megabyte)]
    [InlineData(32 * Sparrow.Global.Constants.Size.Megabyte)]
    public void CanExtendAndNotLooseAnythingExtended(int size) => CanExtendAndNotLooseAnythingBase(size);
    
    private void CanExtendAndNotLooseAnythingBase(int size)
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var growableBuffer = new GrowableBuffer<Progressive>();
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
            Assert.Equal(random2.NextInt64(), results[i]);
        }
        
        int Fill(Span<long> buffer)
        {
            var i = 0;
            for (i = 0; i < buffer.Length && count < size; count++, i++)
                buffer[i] = random.NextInt64();

            return i;
        }
    }
}
