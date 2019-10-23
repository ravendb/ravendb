using System.Dynamic;
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
        public void AsyncMatchesSyncGeneratedIdForDynamicBehavior()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    dynamic client = new ExpandoObject();
                    client.Name = "Test";
                    var result = session.StoreAsync(client);
                    result.Wait();

                    Assert.Equal("ExpandoObjects/1-A", client.Id);
                }
            }
        }

        [Fact]
        public void GeneratedIdForDynamicTagNameAsync()
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

                    var result = session.StoreAsync(client);
                    result.Wait();

                    Assert.Equal("clients/1-A", client.Id);
                }
            }
        }
    }
}
