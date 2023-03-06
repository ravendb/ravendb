using System.Collections.Generic;
using System.Linq;
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
}
