using System.Threading.Tasks;
using Raven.Client;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Core
{
    public class CollectionTests : RavenTestBase
    {
        [Fact(Skip = "Test doesn't work because Raven.Client doesn't know how to handle not json responses")]
        public async Task CanDeleteCollection()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        await session.StoreAsync(new User { Name = "User " + i });
                    }
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.DeleteCollectionAsync("Users");

                using (var session = store.OpenAsyncSession())
                {
                    
                    var users = await session.Query<User>().ToListAsync();
                    Assert.Empty(users);
                }
            }
        }
    }
}
