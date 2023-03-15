using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using Corax.Utils;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Ranking;

public class QuantizationTest : RavenTestBase
{
    public QuantizationTest(ITestOutputHelper output) : base(output)
    {
    }
    
    [Fact]
    public void CanEncodeAndDecodeEveryNumberUnderShort()
    {
        for (short value = 1; value < short.MaxValue; ++value)
        {
            var quantized = EntryIdEncodings.FrequencyQuantization(value);

            Assert.True(quantized <= byte.MaxValue); //In range

            var decoded = EntryIdEncodings.FrequencyReconstructionFromQuantization(quantized);
            Assert.False(decoded <= 0);
        }
    }
    
    [Theory]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(24)]
    [InlineData(32)]
    [InlineData(33)]
    [InlineData(1)]

    public void Avx2InstructionCorrectlyIgnoresFrequency(int size)
    {
        if (Avx2.IsSupported == false && AdvSimd.IsSupported == false)
            return;

        var random = new Random(2337);
        var ids = Enumerable.Range(0, size).Select(i => (long)random.Next(31_111, 59_999)).ToArray();
        
        var idsWithShifted = ids.Select(i => i << 10).ToArray();
        var idsWithShiftedCopy = idsWithShifted.ToArray();
        
        EntryIdEncodings.DecodeAndDiscardFrequencyClassic(idsWithShiftedCopy.AsSpan(), size);
        
        if (Avx2.IsSupported)
            EntryIdEncodings.DecodeAndDiscardFrequencyAvx2(idsWithShifted.AsSpan(), size);
        else if (AdvSimd.IsSupported)
            EntryIdEncodings.DecodeAndDiscardFrequencyNeon(idsWithShifted.AsSpan(), size);

        Assert.Equal(ids, idsWithShifted);
        Assert.Equal(idsWithShifted, idsWithShiftedCopy);
    }
}
