using FastTests;
using Raven.Client.Exceptions;
using Xunit;
using Sparrow.Json;

namespace SlowTests.Issues
{
    public class RavenDB_6607 : RavenTestBase
    {
        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var e = Assert.Throws<RavenException>(() => commands.RawGetJson<BlittableJsonReaderObject>("/queries/dynamic/Products?mapReduce=PricePerUnit-Count-false&mapReduce=Category-None-true"));

                    Assert.Contains("The only valid field name for 'Count' operation is 'Count' but was 'PricePerUnit'", e.Message);
                }
            }
        }
    }
}