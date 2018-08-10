using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Blittable
{
    public class BlittableJsonEqualityTests : NoDisposalNeeded
    {
        [Fact]
        public void Equals_even_though_order_of_properties_is_different()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var json1 = new DynamicJsonValue()
                {
                    ["Age"] = 30,
                    ["Pie"] = 3.147,
                    ["Numbers"] = new DynamicJsonArray()
                    {
                        1, 2, null, 3
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
                    ["Tags"] = null
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
                        1, 2, null, 3
                    },
                    ["Tags"] = null,
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

        [Fact]
        public void GetHashCode_must_not_use_size_if_it_isnt_root()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var json1 = new DynamicJsonValue()
                {
                    ["Address"] = new DynamicJsonValue()
                    {
                        ["City"] = "Atlanta",
                        ["ZipCode"] = 1234
                    },
                    ["Friends"] = "yes"
                };

                var json2 = new DynamicJsonValue()
                {

                    ["Address"] = new DynamicJsonValue()
                    {
                        ["ZipCode"] = 1234,
                        ["City"] = "Atlanta"
                    },
                    ["Friends"] = "no"
                };

                using (var blittable1 = ctx.ReadObject(json1, "foo"))
                using (var blittable2 = ctx.ReadObject(json2, "foo"))
                {
                    blittable1.TryGet("Address", out BlittableJsonReaderObject ob1);
                    blittable2.TryGet("Address", out BlittableJsonReaderObject ob2);

                    HashSet<BlittableJsonReaderObject> items = new HashSet<BlittableJsonReaderObject>();

                    Assert.True(items.Add(ob1));
                    Assert.False(items.Add(ob2));

                    Assert.Equal(ob1.GetHashCode(), ob2.GetHashCode());
                }
            }
        }
    }
}
