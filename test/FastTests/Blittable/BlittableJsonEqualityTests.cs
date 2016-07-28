using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Blittable
{
    public class BlittableJsonEqualityTests
    {
        [Fact]
        public void Equals_even_though_order_of_properties_is_different()
        {
            using (var pool = new UnmanagedBuffersPool("foo"))
            using (var ctx = new JsonOperationContext(pool))
            {
                var json1 = new DynamicJsonValue()
                {
                    ["Age"] = 30,
                    ["Pie"] = 3.147,
                    ["Numbers"] = new DynamicJsonArray()
                    {
                        1, 2, 3
                    },
                    ["Address"] = new DynamicJsonValue()
                    {
                        ["City"] = "Atlanta",
                        ["ZipCode"] = 1234
                    },
                    ["Friends"] = new DynamicJsonArray()
                    {
                        new DynamicJsonValue()
                        {
                            ["Name"] = "James",
                            ["ZipCode"] = 999
                        }
                    },
                };

                var json2 = new DynamicJsonValue()
                {
                    ["Pie"] = 3.147,
                    ["Age"] = 30,
                    ["Address"] = new DynamicJsonValue()
                    {
                        ["ZipCode"] = 1234,
                        ["City"] = "Atlanta"
                    },
                    ["Numbers"] = new DynamicJsonArray()
                    {
                        1, 2, 3
                    },
                    ["Friends"] = new DynamicJsonArray()
                    {
                        new DynamicJsonValue()
                        {
                            ["Name"] = "James",
                            ["ZipCode"] = 999
                        }
                    },
                };

                using (var blittable1 = ctx.ReadObject(json1, "foo"))
                using (var blittable2 = ctx.ReadObject(json2, "foo"))
                {
                    Assert.Equal(blittable1, blittable2);
                }
            }
        }
    }
}