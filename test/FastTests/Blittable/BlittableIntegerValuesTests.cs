using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Blittable
{
    public class BlittableIntegerValuesTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(long.MinValue)]
        [InlineData(-129L)]
        [InlineData(-128L)]
        [InlineData(-1L)]
        [InlineData(0L)]
        [InlineData(1L)]
        [InlineData(300L)]
        [InlineData(1023L)]
        [InlineData(1024L)]
        [InlineData(int.MaxValue)]
        [InlineData(long.MaxValue)]
        public void IntegerPropertiesAndArrayItemsAreReadAsLongs(long number)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var doc = ctx.ReadObject(new DynamicJsonValue
            {
                ["Number"] = number,
                ["Numbers"] = new DynamicJsonArray { number, number }
            }, "doc"))
            {
                Assert.True(doc.TryGetMember("Number", out var value));
                Assert.IsType<long>(value);
                Assert.Equal(number, (long)value);

                Assert.True(doc.TryGet("Numbers", out BlittableJsonReaderArray numbers));
                Assert.IsType<long>(numbers[0]);
                Assert.Equal(number, (long)numbers[0]);
                Assert.Equal(number, (long)numbers[1]);
                Assert.Equal(BlittableJsonToken.Integer, numbers.GetValueTokenTupleByIndex(0).Item2);
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(-128L)]
        [InlineData(0L)]
        [InlineData(7L)]
        [InlineData(1023L)]
        public void SmallIntegersReuseTheSameBox(long number)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var doc = ctx.ReadObject(new DynamicJsonValue { ["A"] = number, ["B"] = number }, "doc"))
            {
                Assert.True(doc.TryGetMember("A", out var a));
                Assert.True(doc.TryGetMember("B", out var b));

                Assert.Same(a, b);
                Assert.Equal(number, (long)a);
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData(-129L)]
        [InlineData(1024L)]
        [InlineData(long.MaxValue)]
        public void LargerIntegersAreBoxedPerRead(long number)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            using (var doc = ctx.ReadObject(new DynamicJsonValue { ["A"] = number, ["B"] = number }, "doc"))
            {
                Assert.True(doc.TryGetMember("A", out var a));
                Assert.True(doc.TryGetMember("B", out var b));

                Assert.NotSame(a, b);
                Assert.Equal(a, b);
                Assert.Equal(number, (long)a);
            }
        }
    }
}
