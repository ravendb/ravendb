using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Client.Indexes;
using Raven.Server.Documents.Indexes;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class LiveIndexingPerformanceCollectorTests : RavenNewTestBase
    {
        private class User
        {
            public string Name { get; set; }
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public override string IndexName
            {
                get { return "Users/ByName"; }
            }

            public UsersByName()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };
            }
        }

        [Fact]
        public async Task CanObtainRecentIndexingPerformance()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                new UsersByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "First"
                    }, "users/1");

                    session.Store(new User
                    {
                        Name = "Second"
                    }, "users/2");

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var database = await GetDocumentDatabaseInstanceFor(store);
                var index = database.IndexStore.GetIndex("Users/ByName");

                var collector = new LiveIndexingPerformanceCollector(database.Changes, database.DatabaseShutdown, new[] {index});

                var tuple = await collector.Queue.TryDequeueAsync(TimeSpan.FromSeconds(1));
                Assert.True(tuple.Item1);
                var stats = tuple.Item2;

                Assert.Equal(1, stats.Length);
                var usersStats = stats[0];
                Assert.Equal("Users/ByName", usersStats.IndexName);

                Assert.Equal(3, usersStats.Performance.Length);

                Assert.Equal(2, usersStats.Performance.Select(x => x.InputCount).Sum());
            }
        }

        [Fact]
        public async Task CanObtainLiveIndexingPerformanceStats()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                new UsersByName().Execute(store);

                var database = await GetDocumentDatabaseInstanceFor(store);
                var index = database.IndexStore.GetIndex("Users/ByName");

                var collector = new LiveIndexingPerformanceCollector(database.Changes, database.DatabaseShutdown, new[] { index });

                var initialIndexing = await collector.Queue.TryDequeueAsync(TimeSpan.FromSeconds(1));
                Assert.True(initialIndexing.Item1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "First"
                    }, "users/1");

                    session.Store(new User
                    {
                        Name = "Second"
                    }, "users/2");

                    session.SaveChanges();
                }

                WaitForIndexing(store);
                

                var tuple = await collector.Queue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                Assert.True(tuple.Item1);
                var stats = tuple.Item2;

                Assert.Equal(1, stats.Length);
                var usersStats = stats[0];
                Assert.Equal("Users/ByName", usersStats.IndexName);

                Assert.Equal(3, usersStats.Performance.Length);

                Assert.Equal(2, usersStats.Performance.Select(x => x.InputCount).Sum());
            }
        }
    }
}