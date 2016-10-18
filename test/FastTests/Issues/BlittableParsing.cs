using System.IO;
using System.Text;
using Newtonsoft.Json.Serialization;
using Sparrow.Json;
using Xunit;

namespace FastTests.Issues
{
    public class BlittableParsing : NoDisposalNeeded
    {
        [Fact]
        public void CanParseProperly()
        {
            var json = "{\"Type\":\"Acknowledge\",\"Etag\":194}";
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                for (int i = 0; i < 3; i++)
                {
                    var multiDocumentParser = context.ParseMultiFrom(new MemoryStream(Encoding.UTF8.GetBytes(json)));
                    var blittableJsonReaderObject = multiDocumentParser.Parse(BlittableJsonDocumentBuilder.UsageMode.None, "n");
                    string s;
                    blittableJsonReaderObject.TryGet("Type", out s);
                    Assert.Equal("Acknowledge", s);
                    context.ResetAndRenew();
                }   
            }
        }
    }
}