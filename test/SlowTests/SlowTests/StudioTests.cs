using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SlowTests
{
    public class StudioTests : RavenTestBase
    {
        public StudioTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CanGetStudioFooterStatistics(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "SomeDoc" });
                    session.SaveChanges();
                }
                await store.ExecuteIndexAsync(new AllowedUsers());
                WaitForIndexing(store);

                var stats = store.Maintenance.Send(new GetStudioFooterStatisticsOperation());

                Assert.NotNull(stats);
                Assert.Equal(1, stats.CountOfIndexes);
                Assert.Equal(2, stats.CountOfDocuments);
                Assert.Equal(0, stats.CountOfIndexingErrors);
                Assert.Equal(0, stats.CountOfStaleIndexes);
            }
        }

        [Theory]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task ShardedCanGetStudioFooterStatistics(Options options)
        {
            //TODO: can unify with non sharded mode after all FooterStatistics fields are decided
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "SomeDoc" });
                    session.SaveChanges();
                }
                await store.ExecuteIndexAsync(new AllowedUsers());
                WaitForIndexing(store, sharded: true);

                var stats = store.Maintenance.Send(new GetStudioFooterStatisticsOperation());

                Assert.NotNull(stats);
                Assert.Equal(1, stats.CountOfIndexes);
                Assert.Equal(2, stats.CountOfDocuments);
                Assert.Equal(null, stats.CountOfIndexingErrors);
                Assert.Equal(null, stats.CountOfStaleIndexes);
            }
        }

        private class User
        {
            public string Name;

            public bool Banned;
        }

        private class AllowedUsers : AbstractIndexCreationTask<User>
        {
            public override string IndexName => "AllowedUsers";

            public AllowedUsers()
            {
                Map = users => from user in users
                               where user.Banned == false
                               select new
                               {
                                   user.Name
                               };
            }
        }
    }
}
