using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Xunit;

namespace FastTests.Blittable
{
    public class ArrayParsingTests : NoDisposalNeeded
    {
        [Fact]
        public async Task CanParseSimpleArray()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var ms = new MemoryStream(Encoding.UTF8.GetBytes("[\"Oren\",\"Arava\"]"));
                var arrayParseResult = await ctx.ParseArrayToMemoryAsync(ms, "array", BlittableJsonDocumentBuilder.UsageMode.None);
                using (arrayParseResult.Item2)
                {
                    Assert.Equal("Oren", arrayParseResult.Item1.GetStringByIndex(0));
                    Assert.Equal("Arava", arrayParseResult.Item1.GetStringByIndex(1));
                }
            }
        }
    }
}