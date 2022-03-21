using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Utils;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16994 : RavenTestBase
    {
        public RavenDB_16994(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ErrorIndexStartupBehavior_ResetAndRestart_Should_Apply_To_Faulty_Indexes_As_Well()
        {
            var path = NewDataPath();
            IOExtensions.DeleteDirectory(path);

            using (var store = GetDocumentStore(new Options
            {
                RunInMemory = false,
                Path = path,
                ModifyDatabaseRecord = databaseRecord =>
                {
                    databaseRecord.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
                }
            }))
            {
                var index = new Companies_ByName();
                await index.ExecuteAsync(store);

                var staticIndexName = index.IndexName;
                string autoIndexName;
                using (var session = store.OpenAsyncSession())
                {
                    _ = await session.Query<Company>()
                        .Statistics(out var stats)
                        .Where(x => x.Name == "HR")
                        .ToListAsync();

                    autoIndexName = stats.IndexName;
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                var staticIndexPath = database.IndexStore.GetIndex(staticIndexName)._environment.Options.BasePath;
                var autoIndexPath = database.IndexStore.GetIndex(autoIndexName)._environment.Options.BasePath;

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                IOExtensions.DeleteFile(staticIndexPath.Combine(Constants.DatabaseFilename).FullPath);
                IOExtensions.DeleteDirectory(staticIndexPath.Combine("Journals").FullPath);

                IOExtensions.DeleteFile(autoIndexPath.Combine(Constants.DatabaseFilename).FullPath);
                IOExtensions.DeleteDirectory(autoIndexPath.Combine("Journals").FullPath);

                database = await Databases.GetDocumentDatabaseInstanceFor(store);

                Assert.Equal(2, database.IndexStore.Count);
                Assert.True(database.IndexStore.GetIndexes().All(x => x is FaultyInMemoryIndex));

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                record.Settings[RavenConfiguration.GetKey(x => x.Indexing.ErrorIndexStartupBehavior)] = IndexingConfiguration.ErrorIndexStartupBehaviorType.ResetAndStart.ToString();

                await store.Maintenance.Server.SendAsync(new UpdateDatabaseOperation(record, record.Etag));

                database = await Databases.GetDocumentDatabaseInstanceFor(store);

                Assert.Equal(2, database.IndexStore.Count);
                Assert.True(database.IndexStore.GetIndexes().All(x => x is FaultyInMemoryIndex == false));
            }
        }

        private class Companies_ByName : AbstractIndexCreationTask<Company>
        {
            public Companies_ByName()
            {
                Map = companies => from company in companies
                                   select new
                                   {
                                       company.Name
                                   };
            }
        }
    }
}
