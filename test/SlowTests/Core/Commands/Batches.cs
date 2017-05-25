using System.Collections.Generic;
using System.Threading.Tasks;

using FastTests;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;
using Xunit;

using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Core.Commands
{
    public class Batches : RavenTestBase
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

                using (var commands = store.Commands())
                {
                    await commands.BatchAsync(new List<ICommandData>
                    {
                        new PutCommandData("users/3", null, new DynamicJsonValue
                        {
                            ["Name"] = "James"
                        }),
                        new PatchCommandData("users/1-A", null, new PatchRequest
                        {
                            Script = "this.Name = 'Nhoj';"
                        }, null),
                        new DeleteCommandData("users/2-A", null)
                    });

                    dynamic multiLoadResult = await commands.GetAsync(new[] { "users/1-A", "users/2-A", "users/3" });

                    Assert.Equal(3, multiLoadResult.Count);

                    var user1 = multiLoadResult[0];
                    var user2 = multiLoadResult[1];
                    var user3 = multiLoadResult[2];

                    Assert.NotNull(user1);
                    Assert.True(user2 == null);
                    Assert.NotNull(user3);

                    Assert.Equal("Nhoj", user1.Name.ToString());
                    Assert.Equal("James", user3.Name.ToString());
                }
            }
        }
    }
}
