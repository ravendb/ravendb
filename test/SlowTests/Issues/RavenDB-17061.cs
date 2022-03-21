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

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

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

        [Fact]
        public async Task Can_project_when_mixed_stored_options_in_index()
        {
            using (var store = GetDocumentStore())
            {
                var index = new UserIndexPartialStore();
                await index.ExecuteAsync(store);

                const string name = "Grisha";
                const string lastName = "Kotler";
                string userId;

                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = name,
                        LastName = lastName
                    };
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();

                    userId = user.Id;
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Query<User, UserIndexPartialStore>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == name)
                        .Select(user => new
                        {
                            user.Name,
                            user.LastName
                        })
                        .ToListAsync();

                    Assert.Equal(1, stats.TotalResults);
                    Assert.Equal(0, stats.SkippedResults);
                    Assert.Equal(1, users.Count);
                    Assert.Equal(name, users[0].Name);
                    Assert.Equal(lastName, users[0].LastName);
                }

                await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(userId);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Query<User, UserIndexPartialStore>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == name)
                        .Select(user => new
                        {
                            user.Name, // stored field
                            user.LastName // not stored field
                        })
                        .ToListAsync();

                    Assert.Equal(1, stats.TotalResults);
                    Assert.Equal(1, stats.SkippedResults);
                    Assert.Equal(0, users.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Query<User, UserIndexPartialStore>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == name)
                        .Select(user => new
                        {
                            user.Name // stored field
                        })
                        .ToListAsync();

                    Assert.Equal(1, stats.TotalResults);
                    Assert.Equal(0, stats.SkippedResults);
                    Assert.Equal(1, users.Count);
                    Assert.Equal(name, users[0].Name);
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

        private class UserIndexPartialStore : AbstractIndexCreationTask<User>
        {
            public UserIndexPartialStore()
            {
                Map = users => from user in users
                    select new
                    {
                        user.Name,
                        user.LastName
                    };

                Store(x => x.Name, FieldStorage.Yes);
            }
        }
    }
}
