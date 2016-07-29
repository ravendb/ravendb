using System.Threading.Tasks;
using FastTests;
using Raven.Json.Linq;
using Xunit;

namespace SlowTests.Tests.Issues
{
    public class RavenDB_3691 : RavenTestBase
    {
        [Fact]
        public async Task CanPutDocumentWithMetadataPropertyBeingNull()
        {
            using (var store = await GetDocumentStore())
            {
                store.DatabaseCommands.Put("test", null, new RavenJObject(), RavenJObject.FromObject(new { Foo = (string)null }));
            }
        }
    }
}
