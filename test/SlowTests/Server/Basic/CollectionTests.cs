using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Basic
{
    public class CollectionTests : RavenTestBase
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

                var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = "FROM Users" }));
                await operation.WaitForCompletionAsync();

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                Assert.Equal(0, stats.CountOfDocuments);
            }
        }
    }
}
