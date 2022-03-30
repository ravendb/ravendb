using System.Dynamic;
using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Async
{
    public class DynamicGeneratedIds : RavenTestBase
    {
        public DynamicGeneratedIds(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task AsyncMatchesSyncGeneratedIdForDynamicBehavior()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    dynamic client = new ExpandoObject();
                    client.Name = "Test";
                    await session.StoreAsync(client);

                    Assert.Equal("ExpandoObjects/1-A", client.Id);
                }
            }
        }

        [Fact]
        public async Task GeneratedIdForDynamicTagNameAsync()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.FindCollectionNameForDynamic = (entity) => entity.EntityName
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    dynamic client = new ExpandoObject();
                    client.Name = "Test";
                    client.EntityName = "clients";

                    await session.StoreAsync(client);

                    Assert.Equal("clients/1-A", client.Id);
                }
            }
        }
    }
}
