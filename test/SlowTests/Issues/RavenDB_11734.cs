using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11734 : RavenTestBase
    {
        private const int USER_COUNT = 500;

        [Fact]
        public async Task Index_Queries_Should_Not_Return_Deleted_Documents()
        {
            using (var store = GetDocumentStore())
            {
                await new UserIndex().ExecuteAsync(store);

                var userIds = new List<string>();
                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < USER_COUNT; i++)
                    {
                        var user = new User { Name = "Test User" };
                        await session.StoreAsync(user);
                        userIds.Add(user.Id);
                    }

                    await session.SaveChangesAsync();
                }

                for (var i = 0; i < USER_COUNT; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        session.Delete(userIds[i]);
                        await session.SaveChangesAsync();
                    }

                    using (var session = store.OpenAsyncSession())
                    {
                        var users = await session.Query<UserIndex.ReduceResult, UserIndex>()
                            .Statistics(out var stats)
                            .Customize(x => x.WaitForNonStaleResults())
                            .OfType<User>()
                            .Select(x => new { x.Id, x.Name })
                            .ToListAsync();

                        Assert.NotEqual(-1, stats.DurationInMs);
                        Assert.Equal(USER_COUNT - i - 1, users.Count);
                    }
                }
            }
        }

        private class UserIndex : AbstractIndexCreationTask<User, UserIndex.ReduceResult>
        {
            public UserIndex()
            {
                Map = users => from user in users
                               select new
                               {
                                   Query = new[] { user.Name },
                                   user.Name,
                                   user.Id
                               };

                Index(x => x.Query, FieldIndexing.Search);
            }

            public class ReduceResult
            {
                public string Query { get; set; }
                public string Name { get; set; }
                public string Id { get; set; }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
