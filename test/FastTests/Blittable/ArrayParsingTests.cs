using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Json;
using Xunit;

namespace FastTests.Blittable
{
    public class ArrayParsingTests
    {
        [Fact]
        public async Task CanParseSimpleArray()
        {
            using (var pool = new UnmanagedBuffersPool("test"))
            using (var ctx = new RavenOperationContext(pool, null))
            {
                var ms = new MemoryStream(Encoding.UTF8.GetBytes("[\"Oren\",\"Arava\"]"));
                var array = await ctx.ParseArrayToMemoryAsync(ms, "array",BlittableJsonDocumentBuilder.UsageMode.None);
                Assert.Equal("Oren", array.GetStringByIndex(0));
                Assert.Equal("Arava", array.GetStringByIndex(1));
            }
        }
    }
}