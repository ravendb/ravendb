using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class LoadAsync : RavenTestBase
    {
        public LoadAsync(ITestOutputHelper output) : base(output)
        {
        }
        
        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Load_Document_And_Expect_Null_User(Options options)
        {
            using var store = GetDocumentStore(options);

            using var session = store.OpenSession();

            string nullId = null;
            var user1 = session.Load<User>(nullId);
            Assert.Null(user1);

            var user2 = session.Load<User>("");
            Assert.Null(user2);
            
            var user3 = session.Load<User>(" ");
            Assert.Null(user3);
        }
        
        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Load_Document_And_Expect_Null_User_Async(Options options)
        {
            using (var store = GetDocumentStore(options))
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
        
        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Load_Document_By_id_Async(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.ClientApi)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Load_Documents_By_ids_Async(Options options)
        {
            using (var store = GetDocumentStore(options))
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
