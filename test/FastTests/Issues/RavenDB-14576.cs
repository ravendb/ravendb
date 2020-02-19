using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

// ReSharper disable IdentifierTypo

namespace FastTests.Issues
{
    public class RavenDB_14576 : RavenTestBase
    {
        public RavenDB_14576(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TypeConverterShouldFlattenArrayOfArrays()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                using var blittable1 = ctx.ReadObject(new DynamicJsonValue { ["Age"] = 1 }, "1");
                using var blittable2 = ctx.ReadObject(new DynamicJsonValue { ["Age"] = 2 }, "2");
                using var blittable3 = ctx.ReadObject(new DynamicJsonValue { ["Age"] = 3 }, "3");

                var arr1 = new DynamicArray(new[] { blittable1, blittable2 });
                var arr2 = new DynamicArray(new[] { blittable3 });
                var array = new DynamicArray(new[] { arr1, arr2 });
                var newArray = arr1.Concat(arr2);

                var flattered = TypeConverter.Flatten(array);
                var count = 0;

                foreach (var bjro in flattered)
                {
                    Assert.Contains(bjro, newArray);
                    count++;
                }

                Assert.Equal(3, count);
            }
        }
    }
}
