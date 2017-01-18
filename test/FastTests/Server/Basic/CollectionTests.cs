using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.NewClient.Operations.Databases;
using Raven.NewClient.Operations.Databases.Documents;
using Xunit;

namespace FastTests.Server.Basic
{
    public class CollectionTests : RavenNewTestBase
    {
        [Fact]
        public async Task CanDeleteCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 1; i <= 10; i++)
                    {
                        await session.StoreAsync(new User { Name = "User " + i }, "users/" + i);
                    }

                    await session.SaveChangesAsync();
                }

                var operation = await store.Operations.SendAsync(new DeleteCollectionOperation("Users"));
                await operation.WaitForCompletionAsync();

                var stats = await store.Admin.SendAsync(new GetStatisticsOperation());

                Assert.Equal(0, stats.CountOfDocuments);
            }
        }
    }
}
