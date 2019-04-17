using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions;
using Sparrow.Json;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_12568 : RavenTestBase
    {
        [Fact]
        public async Task PuttingDocumentWithMissingAttachmentsShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                var requestExecutor = store.GetRequestExecutor();

                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (var stringStream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(Json)))
                using (var blittableJson = context.Read(stringStream, "Reading of foo/bar"))
                {
                    try
                    {
                        requestExecutor.Execute(new PutDocumentCommand("foo/bar", null, blittableJson), context);
                    }
                    catch (RavenException re)
                    {
                        var invalidOperation = re.InnerException as InvalidOperationException;
                        Assert.NotNull(invalidOperation);
                        Assert.Contains("Metadata seems to contain an attachment", invalidOperation.Message);
                    }
                }
            }
        }


        public string Json = @"{
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
