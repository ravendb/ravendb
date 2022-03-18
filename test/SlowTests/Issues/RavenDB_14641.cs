using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14641 : RavenTestBase
    {
        public RavenDB_14641(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task IndexStartupBehaviorTest()
        {
            var path = NewDataPath();

            using (var store = GetDocumentStore(new Options { Path = path, ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.ErrorIndexStartupBehavior)] = IndexingConfiguration.ErrorIndexStartupBehaviorType.Start.ToString() }))
            {
                await IndexStartupBehaviorTestInternal(store);
            }

            using (var store = GetDocumentStore(new Options { Path = path, ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Indexing.ErrorIndexStartupBehavior)] = IndexingConfiguration.ErrorIndexStartupBehaviorType.ResetAndStart.ToString() }))
            {
                await IndexStartupBehaviorTestInternal(store);
            }
        }

        private async Task IndexStartupBehaviorTestInternal(DocumentStore store)
        {
            var index = new Companies_ByName();
            index.Execute(store);

            string autoIndexName = null;

            using (var session = store.OpenSession())
            {
                for (var i = 0; i < 50; i++)
                    session.Store(new Company { Id = i.ToString() });

                session.SaveChanges();

                var companies = session.Query<Company>()
                    .Statistics(out var stats)
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Name != "Test")
                    .ToList();

                autoIndexName = stats.IndexName;
            }

            Indexes.WaitForIndexing(store);

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            var indexInstance = database.IndexStore.GetIndex(index.IndexName);
            indexInstance.SetState(IndexState.Error);

            indexInstance = database.IndexStore.GetIndex(autoIndexName);
            indexInstance.SetState(IndexState.Error);

            var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
            Assert.Equal(IndexState.Error, indexStats.State);

            Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

            indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
            Assert.Equal(IndexState.Normal, indexStats.State);

            indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(autoIndexName));
            Assert.Equal(IndexState.Normal, indexStats.State);
        }

        private class Companies_ByName : AbstractIndexCreationTask<Company>
        {
            public Companies_ByName()
            {
                Map = companies => from c in companies
                                   select new
                                   {
                                       c.Name
                                   };
            }
        }
    }
}
