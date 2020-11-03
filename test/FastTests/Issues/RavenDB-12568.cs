using System.IO;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_12568 : RavenTestBase
    {
        public RavenDB_12568(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void PuttingBlittableWithAttachmentsShouldThrowIfNotExist()
        {
            using (var store = GetDocumentStore())
            {
                var requestExecutor = store.GetRequestExecutor();

                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (var stringStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(json)))
                using (var blittableJson = context.Sync.ReadForDisk(stringStream, "Reading of foo/bar"))
                {
                    Assert.Throws<RavenException>(() => requestExecutor.Execute(new PutDocumentCommand("foo/bar", null, blittableJson), context));
                }
            }
        }

        private string json = @"{
                                    ""CorrelationId"": ""fNVCxBUxMJrUe6SEK3d3UFU8T15c9vxG"",
                                    ""DamageOccuredAt"": ""2018-11-30"",
                                    ""@metadata"": {
                                    ""@attachments"": [
                                        {
                                        ""Name"": ""#{135*707}/lansearch.exe"",
                                        ""Hash"": ""k5sftaB27J00oJVBEguFBbcaUbcUWO8cSezMjrrSIKg="",
                                        ""ContentType"": ""application/x-msdownload"",
                                        ""Size"": 5145720
                                        }
                                    ]
                                    }
                                }";        
    }
}
