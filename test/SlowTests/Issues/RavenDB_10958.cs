using System.IO;
using System.Reflection;
using FastTests;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10958 : RavenTestBase
    {
        public RavenDB_10958(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_write_large_string()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var stream = typeof(RavenDB_10958).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_10958.txt"))
            using (var reader = new StreamReader(stream))
            {
                var content = reader.ReadToEnd();

                var djv = new DynamicJsonValue
                {
                    ["Content"] = content
                };

                var json = context.ReadObject(djv, "test");

                Assert.IsType<LazyStringValue>(json["Content"]);

                var ms = new MemoryStream();

                json.WriteJsonTo(ms);

                ms.Position = 0;
                var result = context.Read(ms, "test");

                Assert.Equal(content, result["Content"].ToString());
            }
        }

        [Fact]
        public void Can_write_large_compressed_stream()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (var stream = typeof(RavenDB_10958).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_10958.json"))
            {
                var json = context.Read(stream, "test");

                Assert.IsType<LazyCompressedStringValue>(json["Content"]);

                var content = json["Content"].ToString();

                var ms = new MemoryStream();

                json.WriteJsonTo(ms);

                ms.Position = 0;
                var result = context.Read(ms, "test");

                Assert.Equal(content, result["Content"].ToString());
            }
        }
    }
}
