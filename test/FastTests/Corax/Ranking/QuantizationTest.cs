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

    public static IEnumerable<object[]> AllShorts => Enumerable.Range(12341, 1).Select(i => new object[] {(short)i});
        // new List<object[]>
        // {
        //     new object[] { 1, 2, 3 },
        //     new object[] { -4, -6, -10 },
        //     new object[] { -2, 2, 0 },
        //     new object[] { int.MinValue, -1, int.MaxValue },
        // };
    
    [Theory]
    [MemberData(nameof(AllShorts))]
    public void CanEncodeAndDecodeEveryNumberUnderShort(short value)
    {
        var quantized = EntryIdEncodings.FrequencyQuantizationMatinsa(value);
        
        Assert.True(quantized <= byte.MaxValue); //In range

        var decoded = EntryIdEncodings.FrequencyDecodeFromQuantization(quantized);
        
        Assert.Equal(value, decoded);
    }
}
