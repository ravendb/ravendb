using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client
{
    public class LoadAsync : RavenTestBase
    {
        [Fact]
        public async Task Load_Document_By_id_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "RavenDB" }, "users/1");
                    await session.SaveChangesAsync();

                    var user = await session.LoadAsync<User>("users/1");
                    Assert.NotNull(user);
                    Assert.Equal(user.Name, "RavenDB");
                }
            }
        }

        [Fact]
        public async Task Load_Documents_By_ids_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "RavenDB" }, "users/1");
                    await session.StoreAsync(new User { Name = "Hibernating Rhinos" }, "users/2");
                    await session.SaveChangesAsync();

                    var user = await session.LoadAsync<User>(new[] { "users/1", "users/2" });
                    Assert.Equal(user.Count, 2);
                }
            }
        }
    }
}
