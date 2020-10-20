using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15739 : RavenTestBase
    {
        public RavenDB_15739(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Search_On_Id()
        {
            using (var store = GetDocumentStore())
            {
                await new UserIndex().ExecuteAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Grisha" }, "users/1");
                    await session.StoreAsync(new User { Name = "Egor" }, "users/2");
                    await session.StoreAsync(new User { Name = "Igal" }, "users/3");
                    await session.SaveChangesAsync();
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var list = await session.Query<UserIndex.Result, UserIndex>()
                        .Search(x => x.Id, "*ser*")
                        .As<User>()
                        .ToListAsync();

                    Assert.Equal(3, list.Count);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class UserIndex : AbstractIndexCreationTask<User>
        {
            public class Result
            {
                public string Id { get; set; }
            }

            public UserIndex()
            {
                Map = users =>
                    from user in users
                    select new
                    {
                        user.Id
                    };
            }
        }

    }
}
