using System.Threading.Tasks;

using FastTests;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace Raven.Tests.Core
{
    public class Crud : RavenTestBase
    {
        [Fact]
        public async Task CanSaveAndLoad()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Fitzchak" });
                    await session.StoreAsync(new User { Name = "Arek" }, "users/arek");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal("Fitzchak", user.Name);

                    user = await session.LoadAsync<User>("users/arek");
                    Assert.NotNull(user);
                    Assert.Equal("Arek", user.Name);
                }
            }
        }

        [Fact]
        public async Task CanOverwriteDocumentWithSmallerValue()
        {
            using (var store = await GetDocumentStore())
            {
                await store.AsyncDatabaseCommands.PutAsync("users/1", null, RavenJObject.FromObject(new User {Name = "Fitzchak", LastName = "Very big value here, so can reproduce the issue"}),
                    RavenJObject.FromObject(new
                    {
                        SomeMoreData = "Make this object bigger",
                        SomeMoreData2 = "Make this object bigger",
                        SomeMoreData3 = "Make this object bigger",
                    }));
                await store.AsyncDatabaseCommands.PutAsync("users/1", null, RavenJObject.FromObject(new User {Name = "Fitzchak" }), null);
            }
        }
    }
}
