using System.IO;
using System.Text;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Blittable
{
    public class BlittableParsing : NoDisposalNeeded
    {
        public BlittableParsing(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanParseProperly()
        {
            var json = "{\"Type\":\"Acknowledge\",\"Etag\":194}";
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                for (int i = 0; i < 3; i++)
                {
                    using (var blittableJsonReaderObject = context.Sync.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes(json)), "n"))
                    {
                        string s;
                        blittableJsonReaderObject.TryGet("Type", out s);
                        Assert.Equal("Acknowledge", s);
                    }
                }   
            }
        }
    }
}
