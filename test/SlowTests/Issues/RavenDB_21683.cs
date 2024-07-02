using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21683 : RavenTestBase
{
    public RavenDB_21683(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
    public async Task Index_Configuration_That_Resets_Index_Should_Change_ResultEtag()
    {
        using (var store = GetDocumentStore(new Options { RunInMemory = false }))
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Company { Name = "HR" });
                await session.SaveChangesAsync();
            }

            var index = new Companies_ByName();
            var indexDefinition = index.CreateIndexDefinition();

            await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));
            Indexes.WaitForIndexing(store);

            using (var session = store.OpenAsyncSession())
            {
                await session
                    .Query<Company, Companies_ByName>()
                    .Statistics(out var initialStats)
                    .ToListAsync();

                // will cause index refresh only
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.MaxTimeForDocumentTransactionToRemainOpen)] = "10";

                await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));
                Indexes.WaitForIndexing(store);

                await session
                    .Query<Company, Companies_ByName>()
                    .Statistics(out var stats)
                    .ToListAsync();

                Assert.Equal(initialStats.ResultEtag, stats.ResultEtag);

                // will cause index reset
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.IndexEmptyEntries)] = "true";

                await store.Maintenance.SendAsync(new PutIndexesOperation(indexDefinition));
                Indexes.WaitForIndexing(store);

                await session
                    .Query<Company, Companies_ByName>()
                    .Statistics(out var lastStats)
                    .ToListAsync();

                Assert.NotEqual(initialStats.ResultEtag, lastStats.ResultEtag);

                // checking if etag is stable after database restart
                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                await session
                    .Query<Company, Companies_ByName>()
                    .Statistics(out stats)
                    .ToListAsync();

                Assert.Equal(lastStats.ResultEtag, stats.ResultEtag);
            }
        }
    }

    private class Companies_ByName : AbstractIndexCreationTask<Company>
    {
        public Companies_ByName()
        {
            Map = companies => from company in companies
                               select new
                               {
                                   Name = company.Name
                               };
        }
    }
}
