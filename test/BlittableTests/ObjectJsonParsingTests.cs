using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Xunit;

namespace BlittableTests
{
    public class ObjectJsonParsingTests
    {
        [Fact]
        public async Task Dup()
        {
            await AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Name"] = "Ayende Rahien"
            },
                "{\"Name\":\"Ayende Rahien\"}");

        }
        [Fact]
        public async Task CanUseNestedObject()
        {
            await AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Wife"] = new DynamicJsonValue
                {
                    ["Name"] = "Rachel"
                }
            },
                "{\"Name\":\"Oren Eini\",\"Wife\":{\"Name\":\"Rachel\"}}");

            
            await AssertEqualAfterRoundTrip(new DynamicJsonValue
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
        public async Task CanGenerateJsonProperly_WithEscapePositions()
        {
            await AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren\r\nEini"
            },
                "{\"Name\":\"Oren\\r\\nEini\"}");
        }

        [Fact]
        public async Task CanGenerateJsonProperly()
        {
            await AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini"
            }, 
                "{\"Name\":\"Oren Eini\"}");

            await AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Age"] = 34,
                ["Married"] = true,
            },
                "{\"Name\":\"Oren Eini\",\"Age\":34,\"Married\":true}");


            await AssertEqualAfterRoundTrip(new DynamicJsonValue
            {
                ["Name"] = "Oren Eini",
                ["Age"] = 34,
                ["Married"] = true,
                ["Null"] = null,
                ["Pie"] = 3.14
            },
                "{\"Name\":\"Oren Eini\",\"Age\":34,\"Married\":true,\"Null\":null,\"Pie\":3.14}");
        }

        private static async Task AssertEqualAfterRoundTrip(DynamicJsonValue  doc, string expected)
        {
            using (var pool = new UnmanagedBuffersPool("foo"))
            using (var ctx = new RavenOperationContext(pool))
            {
                using (var writer = await ctx.ReadObject(doc, "foo"))
                {
                    var memoryStream = new MemoryStream();
                    writer.WriteTo(memoryStream, originalPropertyOrder: true);
                    var actual = Encoding.UTF8.GetString(memoryStream.ToArray());
                    Assert.Equal(expected, actual);
                }
            }
        }
    }

}