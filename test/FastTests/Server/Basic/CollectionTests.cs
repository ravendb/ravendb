using System.Threading.Tasks;

using FastTests;

using Raven.Client;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Core
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
                    for (int i = 1; i <= 10; i++)
                    {
                        await session.StoreAsync(new User { Name = "User " + i },"users/"+i);
                    }
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.DeleteCollectionAsync("Users");


                Assert.Equal(0, store.DatabaseCommands.GetStatistics().CountOfDocuments);
            }
        }
    }
}
