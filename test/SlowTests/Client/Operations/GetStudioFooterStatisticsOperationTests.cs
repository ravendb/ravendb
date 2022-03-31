using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Operations
{
    public class GetStudioFooterStatisticsOperationTests : RavenTestBase
    {
        public GetStudioFooterStatisticsOperationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Studio)]
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

                Indexes.WaitForIndexing(store);

                var stats = store.Maintenance.Send(new GetStudioFooterStatisticsOperation());

                Assert.NotNull(stats);
                Assert.Equal(1, stats.CountOfIndexes);
                Assert.Equal(2, stats.CountOfDocuments);
                Assert.Equal(0, stats.CountOfIndexingErrors);
                Assert.Equal(0, stats.CountOfStaleIndexes);
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
