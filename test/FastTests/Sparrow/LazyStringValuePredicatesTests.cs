using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public class LazyStringValuePredicatesTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        private static readonly string[] Values =
        {
            string.Empty,
            "a",
            "London",
            "companies/17",
            "Zürich",
            "Straße",
            "İstanbul",
            "ABCabcABC",
            "a😀b",
            new string('x', 300) + "London" + new string('y', 300)
        };

        private static readonly string[] Needles = { string.Empty, "on", "ON", "/", "ü", "ss", "ß", "i", "😀", "abc", "London", "zzz" };

        private static readonly StringComparison[] Comparisons =
        {
            StringComparison.Ordinal,
            StringComparison.OrdinalIgnoreCase,
            StringComparison.CurrentCulture,
            StringComparison.CurrentCultureIgnoreCase,
            StringComparison.InvariantCulture,
            StringComparison.InvariantCultureIgnoreCase
        };

        public static IEnumerable<object[]> ValuesData()
        {
            foreach (var value in Values)
                yield return new object[] { value };
        }

        [RavenTheory(RavenTestCategory.Core)]
        [MemberData(nameof(ValuesData))]
        public void StringPredicatesReturnTheSameResultsAsString(string value)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var doc = ctx.ReadObject(new DynamicJsonValue { ["Value"] = value }, "doc"))
            {
                // read from a blittable object, so the value is not materialized as a string
                Assert.True(doc.TryGet("Value", out LazyStringValue lazy));

                foreach (var needle in Needles)
                {
                    Assert.Equal(value.Contains(needle), lazy.Contains(needle));
                    Assert.Equal(value.IndexOf(needle, StringComparison.Ordinal), lazy.IndexOf(needle));
                    Assert.Equal(value.LastIndexOf(needle, StringComparison.Ordinal), lazy.LastIndexOf(needle));

                    foreach (var start in new[] { 0, value.Length / 2, value.Length })
                    {
                        var count = value.Length - start;

                        Assert.Equal(value.IndexOf(needle, start), lazy.IndexOf(needle, start));
                        Assert.Equal(value.IndexOf(needle, start, count), lazy.IndexOf(needle, start, count));
                        Assert.Equal(value.IndexOf(needle, start, count / 2), lazy.IndexOf(needle, start, count / 2));
                    }

                    foreach (var comparison in Comparisons)
                    {
                        var context = $"value: '{value}', needle: '{needle}', comparison: {comparison}";

                        Assert.True(value.Contains(needle, comparison) == lazy.Contains(needle, comparison), context);
                        Assert.True(value.IndexOf(needle, comparison) == lazy.IndexOf(needle, comparison), context);
                        Assert.True(value.LastIndexOf(needle, comparison) == lazy.LastIndexOf(needle, comparison), context);
                        Assert.True(value.EndsWith(needle, comparison) == lazy.EndsWith(needle, comparison), context);

                        // LazyStringValue.StartsWith(string, StringComparison) rejects needles longer than its UTF-8 size upfront
                        if (needle.Length <= lazy.Size)
                            Assert.True(value.StartsWith(needle, comparison) == lazy.StartsWith(needle, comparison), context);

                        foreach (var start in new[] { 0, value.Length / 2, value.Length })
                        {
                            var count = value.Length - start;

                            Assert.True(value.IndexOf(needle, start, comparison) == lazy.IndexOf(needle, start, comparison), context);
                            Assert.True(value.IndexOf(needle, start, count, comparison) == lazy.IndexOf(needle, start, count, comparison), context);
                            Assert.True(value.IndexOf(needle, start, count / 2, comparison) == lazy.IndexOf(needle, start, count / 2, comparison), context);
                        }

                        if (needle.Length > 0)
                        {
                            var c = needle[0];
                            Assert.True(value.Contains(c) == lazy.Contains(c), context);
                            Assert.True(value.Contains(c, comparison) == lazy.Contains(c, comparison), context);
                            Assert.True(value.IndexOf(c, comparison) == lazy.IndexOf(c, comparison), context);
                        }
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Core)]
        public void InvalidArgumentsThrowTheSameExceptionsAsString()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var doc = ctx.ReadObject(new DynamicJsonValue { ["Value"] = "London" }, "doc"))
            {
                Assert.True(doc.TryGet("Value", out LazyStringValue lazy));

                Assert.Throws<ArgumentNullException>(() => lazy.Contains(null));
                Assert.Throws<ArgumentNullException>(() => lazy.Contains(null, StringComparison.Ordinal));
                Assert.Throws<ArgumentNullException>(() => lazy.IndexOf((string)null));
                Assert.Throws<ArgumentNullException>(() => lazy.IndexOf(null, StringComparison.OrdinalIgnoreCase));
                Assert.Throws<ArgumentNullException>(() => lazy.IndexOf(null, 1));
                Assert.Throws<ArgumentNullException>(() => lazy.LastIndexOf((string)null));
                Assert.Throws<ArgumentNullException>(() => lazy.LastIndexOf(null, StringComparison.Ordinal));
                Assert.Throws<ArgumentNullException>(() => lazy.EndsWith(null, StringComparison.Ordinal));

                Assert.Throws<ArgumentOutOfRangeException>(() => lazy.IndexOf("on", -1));
                Assert.Throws<ArgumentOutOfRangeException>(() => lazy.IndexOf("on", 7));
                Assert.Throws<ArgumentOutOfRangeException>(() => lazy.IndexOf("on", 2, 5));
                Assert.Throws<ArgumentOutOfRangeException>(() => lazy.IndexOf("on", 2, -1, StringComparison.Ordinal));

                Assert.Throws<ArgumentException>(() => lazy.IndexOf("on", (StringComparison)42));
            }
        }
    }
}
