using System.IO;
using System.Text;

using Raven.Abstractions.Data;

using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Blittable
{
    public class ObjectJsonParsingTests
    {
        [Fact]
        public void Zzz()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var array = new DynamicJsonArray
                {
                    new DynamicJsonValue
                    {
                        ["LastName"] = "John"
                    }
                };

                var input = new DynamicJsonValue
                {
                    ["$values"] = array
                };

                using (var inputJson = ctx.ReadObject(input, "input", BlittableJsonDocumentBuilder.UsageMode.CompressSmallStrings))
                {

                }
            }
        }


        [Fact]
        public void CanCompressSmallStrings()
        {
            var traverser = new BlittableJsonTraverser();

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var input = new DynamicJsonValue
                {
                    [Constants.DocumentIdFieldName] = "tracks/1",
                    ["Title"] = "A and G motor vehicles"
                };

                using (var inputJson = ctx.ReadObject(input, "input", BlittableJsonDocumentBuilder.UsageMode.CompressSmallStrings))
                {
                    var output = new DynamicJsonValue
                    {
                        [Constants.DocumentIdFieldName] = "tracks/1",
                    };

                    object value;
                    traverser.TryRead(inputJson, "Title", out value);

                    output["Title"] = value;

                    using (var outputJson = ctx.ReadObject(output, "output", BlittableJsonDocumentBuilder.UsageMode.CompressSmallStrings))
                    {
                        Assert.Equal(inputJson.ToString(), outputJson.ToString());
                    }
                }
            }
        }

        [Fact]
        public void Dup()
        {
            AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Name"] = "Ayende Rahien"
            },
                "{\"Name\":\"Ayende Rahien\"}");

        }
        [Fact]
        public void CanUseNestedObject()
        {
            AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Wife"] = new DynamicJsonValue
                {
                    ["Name"] = "Rachel"
                }
            },
                "{\"Name\":\"Oren Eini\",\"Wife\":{\"Name\":\"Rachel\"}}");


            AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Dogs"] = new DynamicJsonArray
                {
                    "Arava",
                    "Oscar"
                }
            },
                "{\"Name\":\"Oren Eini\",\"Dogs\":[\"Arava\",\"Oscar\"]}");
        }

        [Fact]
        public void CanGenerateJsonProperly_WithEscapePositions()
        {
            AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren\r\nEini"
            },
                "{\"Name\":\"Oren\\r\\nEini\"}");
        }

        [Fact]
        public void CanGenerateJsonProperly()
        {
            AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini"
            },
                "{\"Name\":\"Oren Eini\"}");

            AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Age"] = 34,
                ["Married"] = true,
            },
                "{\"Name\":\"Oren Eini\",\"Age\":34,\"Married\":true}");


            AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Age"] = 34,
                ["Married"] = true,
                ["Null"] = null,
                ["Pie"] = 3.14
            },
                "{\"Name\":\"Oren Eini\",\"Age\":34,\"Married\":true,\"Null\":null,\"Pie\":3.14}");
        }

        private static void AssertEqualAfterRoundTrip(DynamicJsonValue doc, string expected)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                using (var writer = ctx.ReadObject(doc, "foo"))
                {
                    var memoryStream = new MemoryStream();
                    ctx.Write(memoryStream, writer);
                    var actual = Encoding.UTF8.GetString(memoryStream.ToArray());
                    Assert.Equal(expected, actual);
                }
            }
        }
    }

}