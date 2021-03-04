using System.IO;
using System.Text;
using FastTests;
using Raven.Server.Documents.Indexes.Static;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class StringIsNullOrEmpty : NoDisposalNeeded
    {
        public StringIsNullOrEmpty(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes("{}")))
                {
                    var json = context.Sync.ReadForMemory(stream, "json");

                    dynamic doc = new DynamicBlittableJson(json);

                    Assert.True(string.IsNullOrEmpty(doc.Name));
                }
            }
        }
    }
}
