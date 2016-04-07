using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace FastTests.Blittable
{
    public class ObjectJsonParsingTests
    {
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

        private static void AssertEqualAfterRoundTrip(DynamicJsonValue  doc, string expected)
        {
            using (var pool = new UnmanagedBuffersPool("foo"))
            using (var ctx = new JsonOperationContext(pool))
            {
                using (var writer = ctx.ReadObject(doc, "foo"))
                {
                    var memoryStream = new MemoryStream();
                    ctx.WriteOrdered(memoryStream, writer);
                    var actual = Encoding.UTF8.GetString(memoryStream.ToArray());
                    Assert.Equal(expected, actual);
                }
            }
        }
    }

}