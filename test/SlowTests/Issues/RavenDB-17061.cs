using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17061 : RavenTestBase
    {
        public RavenDB_17061(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_project_when_the_document_is_missing()
        {
            using (var store = GetDocumentStore())
            {
                const string name = "Grisha";
                string userId;

                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = name
                    };
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                    
                    userId = user.Id;
                }

                WaitForIndexing(store);

                QueryStatistics stats;

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Query<User>()
                        .Statistics(out stats)
                        .Where(x => x.Name == name)
                        .Select(x => x.Id)
                        .ToListAsync();

                    Assert.Equal(1, stats.TotalResults);
                    Assert.Equal(0, stats.SkippedResults);
                    Assert.Equal(1, users.Count);
                    Assert.Equal(userId, users[0]);
                }

                await store.Maintenance.SendAsync(new StopIndexOperation(stats.IndexName));

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(userId);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Query<User>()
                        .Statistics(out stats)
                        .Where(x => x.Name == name)
                        .Select(x => x.Id)
                        .ToListAsync();

                    Assert.Equal(1, stats.TotalResults);
                    Assert.Equal(1, stats.SkippedResults);
                    Assert.Equal(0, users.Count);
                }
            }
        }

        [Fact]
        public async Task Can_project_when_the_document_is_missing_with_index()
        {
            using (var store = GetDocumentStore())
            {
                var index = new UserIndex();
                await index.ExecuteAsync(store);

                const string name = "Grisha";
                string userId;

                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = name
                    };
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();

                    userId = user.Id;
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Query<User, UserIndex>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == name)
                        .Select(x => x.Id)
                        .ToListAsync();

                    Assert.Equal(1, stats.TotalResults);
                    Assert.Equal(0, stats.SkippedResults);
                    Assert.Equal(1, users.Count);
                    Assert.Equal(userId, users[0]);
                }

                await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(userId);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Query<User, UserIndex>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == name)
                        .Select(x => x.Name) // projected from the index
                        .ToListAsync();

                    Assert.Equal(1, stats.TotalResults);
                    Assert.Equal(0, stats.SkippedResults);
                    Assert.Equal(1, users.Count);
                    Assert.Equal(name, users[0]);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Query<User, UserIndex>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == name)
                        .Select(x => x.Id) // projected from the document
                        .ToListAsync();

                    Assert.Equal(1, stats.TotalResults);
                    Assert.Equal(1, stats.SkippedResults);
                    Assert.Equal(0, users.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Query<User, UserIndex>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == name)
                        .Select(x => new
                        {
                            x.Id, // projected from the document
                            x.Name // projected from the index
                        }) 
                        .ToListAsync();

                    Assert.Equal(1, stats.TotalResults);
                    Assert.Equal(1, stats.SkippedResults);
                    Assert.Equal(0, users.Count);
                }
            }
        }

        private class UserIndex : AbstractIndexCreationTask<User>
        {
            public UserIndex()
            {
                Map = users => from user in users
                    select new
                    {
                        user.Name
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
