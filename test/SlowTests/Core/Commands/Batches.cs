using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit.Abstractions;

using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations;
using Sparrow.Json.Parsing;
using Xunit;

using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Core.Commands
{
    public class Batches : RavenTestBase
    {
        public Batches(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public async Task CanDoBatchOperations(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
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

        [Fact]
        public async Task CanDoBatchOperationsWithBatchIdentityRequest()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    await commands.BatchAsync(new List<ICommandData>
                    {
                        // identities: users/1 users/2
                        new PutCommandData("users|", null, new DynamicJsonValue
                        {
                            ["Name"] = "James1"
                        }),
                        new PutCommandData("users|", null, new DynamicJsonValue
                        {
                            ["Name"] = "James2"
                        }),
                        // mix some regular docs:
                        new PutCommandData("users/123", null, new DynamicJsonValue
                        {
                            ["Name"] = "James123"
                        }),
                        new PatchCommandData("users/1-A", null, new PatchRequest
                        {
                            Script = "this.Name = 'Nhoj';"
                        }, null),
                        new DeleteCommandData("users/2-A", null),

                        // identities: users/3 users/4
                        new PutCommandData("users|", null, new DynamicJsonValue
                        {
                            ["Name"] = "James3"
                        }),
                        new PutCommandData("users|", null, new DynamicJsonValue
                        {
                            ["Name"] = "James4"
                        }),
                    });

                    dynamic multiLoadResult = await commands.GetAsync(0, 25);

                    Assert.Equal(5, multiLoadResult.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user1 = await session.LoadAsync<User>("users/1");
                    var user2 = await session.LoadAsync<User>("users/2");
                    var user3 = await session.LoadAsync<User>("users/3");
                    var user4 = await session.LoadAsync<User>("users/4");
                    var user123 = await session.LoadAsync<User>("users/123");

                    Assert.Equal("James1", user1.Name);
                    Assert.Equal("James2", user2.Name);
                    Assert.Equal("James3", user3.Name);
                    Assert.Equal("James4", user4.Name);
                    Assert.Equal("James123", user123.Name);
                }
            }
        }  
    }
}
