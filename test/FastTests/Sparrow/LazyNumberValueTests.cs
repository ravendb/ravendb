using System;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public class LazyNumberValueTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("42", 42L, 42UL, 42d, 42f)]
        [InlineData("-7", -7L, 0UL, -7d, -7f)] // ulong falls back to the double conversion, which saturates
        [InlineData("1.5", 1L, 1UL, 1.5d, 1.5f)]
        [InlineData("1.25", 1L, 1UL, 1.25d, 1.25f)]
        [InlineData("0.1", 0L, 0UL, 0.1d, 0.1f)]
        [InlineData("18446744073709551615", 9223372036854775807L, 18446744073709551615UL, 18446744073709551615d, 18446744073709551615f)]
        [InlineData("1E+10", 10000000000L, 10000000000UL, 1E+10d, 1E+10f)]
        public void ConversionsReturnTheSameValuesInAnyOrder(string text, long expectedLong, ulong expectedULong, double expectedDouble, float expectedFloat)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                // every conversion on a fresh instance
                Assert.Equal(expectedLong, (long)Create(ctx, text));
                Assert.Equal(expectedULong, (ulong)Create(ctx, text));
                Assert.Equal(expectedDouble, (double)Create(ctx, text));
                Assert.Equal(expectedFloat, (float)Create(ctx, text));
                var expectedDecimal = (decimal)Create(ctx, text);

                // all conversions on the same instance, in both orders and repeated (cached)
                var forward = Create(ctx, text);
                var backward = Create(ctx, text);

                for (var round = 0; round < 2; round++)
                {
                    Assert.Equal(expectedLong, (long)forward);
                    Assert.Equal(expectedULong, (ulong)forward);
                    Assert.Equal(expectedDouble, (double)forward);
                    Assert.Equal(expectedFloat, (float)forward);
                    Assert.Equal(expectedDecimal, (decimal)forward);

                    Assert.Equal(expectedDecimal, (decimal)backward);
                    Assert.Equal(expectedFloat, (float)backward);
                    Assert.Equal(expectedDouble, (double)backward);
                    Assert.Equal(expectedULong, (ulong)backward);
                    Assert.Equal(expectedLong, (long)backward);
                }

                Assert.Equal(text, (string)forward);
                Assert.Equal(text, forward.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void DecimalConversionIsExact()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                Assert.Equal(1.25m, (decimal)Create(ctx, "1.25"));
                Assert.Equal(0.1m, (decimal)Create(ctx, "0.1"));
                Assert.Equal(-7m, (decimal)Create(ctx, "-7"));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void InvalidNumberThrowsOnConversion()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                Assert.Throws<InvalidCastException>(() => (double)Create(ctx, "abc"));
                Assert.Throws<InvalidCastException>(() => (decimal)Create(ctx, "abc"));
                Assert.Throws<InvalidCastException>(() => (long)Create(ctx, "abc"));
                Assert.Throws<InvalidCastException>(() => (ulong)Create(ctx, "abc"));
                Assert.Throws<InvalidCastException>(() => (float)Create(ctx, "abc"));
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void EqualityDependsOnTheCachedDoubleOrDecimalValues()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                // nothing converted yet - compared by the text
                var a = Create(ctx, "1");
                var b = Create(ctx, "01");
                Assert.False(a.Equals(b));
                Assert.Equal(a.Inner.GetHashCode(), a.GetHashCode());

                // long / ulong / float conversions don't participate in equality
                // (integers parse directly, so these don't fall back to the double conversion)
                _ = (long)a;
                _ = (long)b;
                _ = (ulong)a;
                _ = (float)b;
                Assert.False(a.Equals(b));
                Assert.Equal(a.Inner.GetHashCode(), a.GetHashCode());

                // a long conversion of a fractional number falls back to the double conversion, which is cached as well
                var g = Create(ctx, "1.0");
                var h = Create(ctx, "1.00");
                _ = (long)g;
                _ = (long)h;
                Assert.True(g.Equals(h));

                // decimal on both sides - compared as decimals
                _ = (decimal)a;
                _ = (decimal)b;
                Assert.True(a.Equals(b));
                Assert.Equal(1.0m.GetHashCode(), a.GetHashCode());

                // double on both sides takes precedence
                var c = Create(ctx, "1.0");
                var d = Create(ctx, "1.00");
                _ = (double)c;
                Assert.False(c.Equals(d));
                _ = (double)d;
                Assert.True(c.Equals(d));
                Assert.Equal(1.0d.GetHashCode(), c.GetHashCode());

                // decimal cached on one side only - compared by the text
                var e = Create(ctx, "2.0");
                var f = Create(ctx, "2.00");
                _ = (decimal)e;
                Assert.False(e.Equals(f));
                Assert.True(e == Create(ctx, "2.0"));
                Assert.True(e != f);
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void SpecialValuesAreDetectedBeforeAndAfterConversion()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var nan = Create(ctx, "NaN");
                Assert.True(nan.IsNaN());
                Assert.False(nan.IsPositiveInfinity());

                var positive = Create(ctx, "Infinity");
                Assert.True(positive.IsPositiveInfinity());
                Assert.False(positive.IsNegativeInfinity());

                var negative = Create(ctx, "-Infinity");
                Assert.True(negative.IsNegativeInfinity());
                Assert.False(negative.IsNaN());

                var regular = Create(ctx, "3.5");
                Assert.False(regular.IsNaN());
                Assert.Equal(3.5d, (double)regular);
                Assert.False(regular.IsNaN());
                Assert.False(regular.IsPositiveInfinity());
                Assert.False(regular.IsNegativeInfinity());
            }
        }

        private static LazyNumberValue Create(JsonOperationContext ctx, string text) => new(ctx.GetLazyString(text));
    }
}
