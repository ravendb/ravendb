using System.Threading.Tasks;

using FastTests;

using Raven.Abstractions.Commands;
using Raven.Client.Data;
using Raven.Json.Linq;

using Xunit;

using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Core.Commands
{
    public class Batches : RavenTestBase
    {
        [Fact]
        public async Task CanDoBatchOperations()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John" });
                    await session.StoreAsync(new User { Name = "Dave" });
                    await session.SaveChangesAsync();
                }

                await store.AsyncDatabaseCommands.BatchAsync(new ICommandData[]
                {
                    new PutCommandData
                    {
                        Document = new RavenJObject {{"Name", "James"}},
                        Key = "users/3"
                    },
                    new PatchCommandData
                    {
                        Key = "users/1",
                        Patch = new PatchRequest
                        {
                            Script = "this.Name = 'Nhoj';"
                        }
                    },
                    new DeleteCommandData()
                    {
                        Key = "users/2"
                    },
                });

                var multiLoadResult = await store.AsyncDatabaseCommands.GetAsync(new[] { "users/1", "users/2", "users/3" }, null);

                Assert.Equal(3, multiLoadResult.Results.Count);

                var user1 = multiLoadResult.Results[0];
                var user2 = multiLoadResult.Results[1];
                var user3 = multiLoadResult.Results[2];

                Assert.NotNull(user1);
                Assert.Null(user2);
                Assert.NotNull(user3);

                Assert.Equal("Nhoj", user1.Value<string>("Name"));
                Assert.Equal("James", user3.Value<string>("Name"));
            }
        }
    }
}
