using System.Dynamic;
using Xunit;

namespace NewClientTests.NewClient.Raven.Tests.Bugs.Async
{
    public class DynamicGeneratedIds : RavenTestBase
    {
        [Fact(Skip = "NotImplementedException")]
        public void AsyncMatchesSyncGeneratedIdForDynamicBehavior()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenNewAsyncSession())
                {
                    dynamic client = new ExpandoObject();
                    client.Name = "Test";
                    var result = session.StoreAsync(client);
                    result.Wait();

                    Assert.Equal("ExpandoObjects/1", client.Id);
                }
            }
        }

        [Fact(Skip = "NotImplementedException")]
        public void GeneratedIdForDynamicTagNameAsync()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.FindDynamicTagName = (entity) => entity.EntityName;

                using (var session = store.OpenNewAsyncSession())
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
