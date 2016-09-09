using FastTests;
using Raven.Client.Linq;
using Raven.Json.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class StringIsNullOrEmpty : NoDisposalNeeded
    {
        [Fact]
        public void ShouldWork()
        {
            dynamic doc = new DynamicJsonObject(new RavenJObject());

            Assert.True(string.IsNullOrEmpty(doc.Name));
        }
    }
}
