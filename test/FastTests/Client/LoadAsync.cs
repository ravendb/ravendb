using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class LoadAsync : RavenTestBase
    {
        public LoadAsync(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public void Load_Document_And_Expect_Null_User()
        {
            using var store = GetDocumentStore();

            using var session = store.OpenSession();

            string nullId = null;
            var user1 = session.Load<User>(nullId);
            Assert.Null(user1);

            var user2 = session.Load<User>("");
            Assert.Null(user2);
            
            var user3 = session.Load<User>(" ");
            Assert.Null(user3);
        }
        
        [Fact]
        public async Task Load_Document_And_Expect_Null_User_Async()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    string nullId = null;
                    var user1 =await session.LoadAsync<User>(nullId);
                    Assert.Null(user1);
                    
                    var user2 = await session.LoadAsync<User>("");
                    Assert.Null(user2);
            
                    var user3 = await session.LoadAsync<User>(" ").ConfigureAwait(false);
                    Assert.Null(user3);
                }
            }
        }
        
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
