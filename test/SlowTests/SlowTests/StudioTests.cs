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
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
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

                if (options.DatabaseMode == RavenDatabaseMode.Single)
                {
                    Assert.Equal(0, stats.CountOfIndexingErrors);
                    Assert.Equal(0, stats.CountOfStaleIndexes);
                }
                else
                {
                    Assert.Equal(null, stats.CountOfIndexingErrors);
                    Assert.Equal(null, stats.CountOfStaleIndexes);
                }
            }
        }
        
        private class User
        {
            public string Name;
        }

        private class AllowedUsers : AbstractIndexCreationTask<User>
        {
            public override string IndexName => "AllowedUsers";

            public AllowedUsers()
            {
                Map = users => from user in users
                    select new
                               {
                                   user.Name
                               };
            }
        }
    }
}
