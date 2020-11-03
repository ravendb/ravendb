using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable
{
    public class ObjectJsonParsingTests : NoDisposalNeeded
    {
        public ObjectJsonParsingTests(ITestOutputHelper output) : base(output)
        {
        }

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
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var input = new DynamicJsonValue
                {
                    [Constants.Documents.Indexing.Fields.DocumentIdFieldName] = "tracks/1",
                    ["Title"] = "A and G motor vehicles"
                };

                using (var inputJson = ctx.ReadObject(input, "input", BlittableJsonDocumentBuilder.UsageMode.CompressSmallStrings))
                {
                    var output = new DynamicJsonValue
                    {
                        [Constants.Documents.Indexing.Fields.DocumentIdFieldName] = "tracks/1",
                    };

                    var value = BlittableJsonTraverser.Default.Read(inputJson, "Title");

                    output["Title"] = value;

                    using (var outputJson = ctx.ReadObject(output, "output", BlittableJsonDocumentBuilder.UsageMode.CompressSmallStrings))
                    {
                        Assert.Equal(inputJson.ToString(), outputJson.ToString());
                    }
                }
            }
        }

        [Fact]
        public Task Dup()
        {
            return AssertEqualAfterRoundTripAsync(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Name"] = "Ayende Rahien"
            },
                "{\"Name\":\"Ayende Rahien\"}");
        }

        [Fact]
        public async Task CanUseNestedObject()
        {
            await AssertEqualAfterRoundTripAsync(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Wife"] = new DynamicJsonValue
                {
                    ["Name"] = "Rachel"
                }
            },
                "{\"Name\":\"Oren Eini\",\"Wife\":{\"Name\":\"Rachel\"}}");

            await AssertEqualAfterRoundTripAsync(new DynamicJsonValue
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
        public Task CanGenerateJsonProperly_WithEscapePositions()
        {
            return AssertEqualAfterRoundTripAsync(new DynamicJsonValue
            {
                ["Name"] = "Oren\r\nEini"
            },
                "{\"Name\":\"Oren\\r\\nEini\"}");
        }

        [Fact]
        public async Task CanGenerateJsonProperly()
        {
            await AssertEqualAfterRoundTripAsync(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini"
            },
                "{\"Name\":\"Oren Eini\"}");

            await AssertEqualAfterRoundTripAsync(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Age"] = 34,
                ["Married"] = true,
            },
                "{\"Name\":\"Oren Eini\",\"Age\":34,\"Married\":true}");

            await AssertEqualAfterRoundTripAsync(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Age"] = 34,
                ["Married"] = true,
                ["Null"] = null,
                ["Pie"] = 3.14
            },
                "{\"Name\":\"Oren Eini\",\"Age\":34,\"Married\":true,\"Null\":null,\"Pie\":3.14}");
        }

        private static async Task AssertEqualAfterRoundTripAsync(DynamicJsonValue doc, string expected)
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                using (var writer = ctx.ReadObject(doc, "foo"))
                {
                    var memoryStream = new MemoryStream();
                    await ctx.WriteAsync(memoryStream, writer);
                    var actual = Encoding.UTF8.GetString(memoryStream.ToArray());
                    Assert.Equal(expected, actual);
                }
            }
        }
    }
}
