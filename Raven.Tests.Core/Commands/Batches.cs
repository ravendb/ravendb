using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.Commands
{
    public class Batches : RavenCoreTestBase
    {
        [Fact]
        public async Task CanDoBatchOperations()
        {
            using (var store = GetDocumentStore())
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
                        Metadata = new RavenJObject(),
                        Key = "users/3"
                    },
                    new PatchCommandData()
                    {
                        Key = "users/1",
                        Patches = new[]
                        {
                            new PatchRequest
                            {
                                Name = "Name",
                                Type = PatchCommandType.Set,
                                Value = "Nhoj"
                            }
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
