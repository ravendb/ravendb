using Raven.Server.Documents.Indexes.Persistence;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class TrimTrailingZerosTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData("1.50", "1.5")]
        [InlineData("0.10", "0.1")]
        [InlineData("-3.10", "-3.1")]
        [InlineData("2.0", "2")]
        [InlineData("2.000", "2")]
        [InlineData("120.0", "120")]
        [InlineData("100.200", "100.2")]
        public void TrailingZerosAreTrimmed(string number, string expected)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var value = new LazyNumberValue(ctx.GetLazyString(number));

                Assert.True(ConverterBase.TryToTrimTrailingZeros(value, ctx, out var trimmed));
                Assert.Equal(expected, trimmed.ToString());
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [InlineData("10")]
        [InlineData("100")]
        [InlineData("-100")]
        [InlineData("100.25")]
        [InlineData("0.5")]
        [InlineData(".50")]
        public void NumbersWithoutTrailingZerosAfterTheDotAreNotTrimmed(string number)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var value = new LazyNumberValue(ctx.GetLazyString(number));

                Assert.False(ConverterBase.TryToTrimTrailingZeros(value, ctx, out var trimmed));
                Assert.Null(trimmed);
            }
        }
    }
}
