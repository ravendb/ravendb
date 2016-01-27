using System.Collections.Generic;
using System.IO;
using System.Text;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Xunit;

namespace BlittableTests
{
    public unsafe class ObjectJsonParsingTests
    {
        [Fact]
        public void Dup()
        {
            //var d= new DynamicJsonBuilder();
            //d["Name"] = "Oren Eini";
            //d["Name"] = "Ayende Rahien";

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
            using (var ctx = new RavenOperationContext(pool))
            {
                var writer = ctx.ReadObject(doc, "foo");
                var allocatedMemoryData = pool.Allocate(writer.SizeInBytes);
                try
                {
                    var address = (byte*) allocatedMemoryData.Address;
                    writer.CopyTo(address);

                    var readerObject = new BlittableJsonReaderObject(address, writer.SizeInBytes, ctx);
                    var memoryStream = new MemoryStream();
                    readerObject.WriteTo(memoryStream, originalPropertyOrder: true);
                    var actual = Encoding.UTF8.GetString(memoryStream.ToArray());
                    Assert.Equal(expected, actual);
                }
                finally
                {
                    pool.Return(allocatedMemoryData);
                }
            }
        }
    }

}