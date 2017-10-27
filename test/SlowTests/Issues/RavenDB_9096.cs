using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9096 : RavenTestBase
    {
        [Fact]
        public void LongMinShouldBeParsedCorrectly()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var djv = new DynamicJsonValue
                {
                    ["Value"] = long.MinValue
                };

                var json = context.ReadObject(djv, "json");

                Assert.True(json.TryGetMember("Value", out var value));
                Assert.Equal(long.MinValue, value);

                var s = json.ToString();

                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(s)))
                {
                    json = context.ReadForMemory(stream, "json");

                    Assert.True(json.TryGet("Value", out LazyNumberValue lnv));

                    Assert.Equal(long.MinValue, lnv.ToInt64(CultureInfo.InvariantCulture));
                }
            }
        }
    }
}
