using System.Dynamic;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Bugs.Async
{
    public class DynamicGeneratedIds : RavenNewTestBase
    {
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

                    Assert.Equal("ExpandoObjects/1", client.Id);
                }
            }
        }

        [Fact]
        public void GeneratedIdForDynamicTagNameAsync()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.FindDynamicTagName = (entity) => entity.EntityName;

                using (var session = store.OpenAsyncSession())
                {
                    dynamic client = new ExpandoObject();
                    client.Name = "Test";
                    client.EntityName = "clients";

                    var result = session.StoreAsync(client);
                    result.Wait();

                    Assert.Equal("clients/1", client.Id);
                }
            }
        }
    }
}
