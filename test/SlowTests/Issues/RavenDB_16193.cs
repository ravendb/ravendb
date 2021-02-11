using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16193 : RavenTestBase
    {
        public RavenDB_16193(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public async Task WillGetIndexStatsFromStorageOrReader()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new StopIndexingOperation());

                var index = new Users_ByName();
                await index.ExecuteAsync(store);

                var indexInstance = (await GetDatabase(store.Database)).IndexStore.GetIndex(index.IndexName);

                // the indexing was never run so it didn't store entries count in the stats
                // in that case it will open the index reader under the covers to get that count

                IndexStats indexStats = await store.Maintenance.SendAsync(new GetIndexStatisticsOperation(index.IndexName));

                Assert.Equal(0, indexStats.EntriesCount);

                Assert.NotNull(indexInstance.IndexPersistence._lastReader);

                using (var session = store.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Arek"
                    });

                    session.SaveChanges();
                }

                await store.Maintenance.SendAsync(new StartIndexingOperation());

                WaitForIndexing(store);

                // let's force to clean the reader
                indexInstance.IndexPersistence.Clean(CleanupMode.Deep);

                Assert.Null(indexInstance.IndexPersistence._lastReader);

                // after the indexing batch has been run we'll get the entries count directly from the storage so no need to recreate the index reader

                Assert.Equal(1, WaitForEntriesCount(store, index.IndexName, 1));

                Assert.Null(indexInstance.IndexPersistence._lastReader);
            }
        }

        private class User
        {
            public string Name { get; set; }
        }

        private class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from user in users
                    select new { user.Name };
            }
        }
    }
}
