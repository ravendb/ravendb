using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12269 : RavenTestBase
    {
        [Fact]
        public async Task Changing_index_def_must_not_error_index()
        {
            using (var store = GetDocumentStore(new Options
            {
                Path = NewDataPath()
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Joe" });
                    await session.StoreAsync(new User { Name = "Doe" });

                    await session.SaveChangesAsync();
                }

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps = { "from user in docs.Users select new { user.Name }" },
                    Type = IndexType.Map,
                    Name = "Users_ByName"
                }));

                WaitForIndexing(store);

                var db = await GetDatabase(store.Database);

                db.IndexStore.StopIndexing();

                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Maps =
                    {
                        "from user in docs.Users select new { DifferentName = user.Name }"
                    },
                    Type = IndexType.Map,
                    Name = "Users_ByName"
                }));

                
                var replacementIndex = db.IndexStore.GetIndex("ReplacementOf/Users_ByName");

                // let's try to force calling storageEnvironment.Cleanup() inside ExecuteIndexing method
                replacementIndex.LowMemory();
                replacementIndex._indexStorage.Environment().LogsApplied();

                db.IndexStore.StartIndexing();

                WaitForIndexing(store);

                // let's try to force calling storageEnvironment.Cleanup() inside ExecuteIndexing method
                replacementIndex.LowMemory();
                replacementIndex._indexStorage.Environment().LogsApplied();

                Thread.Sleep(500);

                Assert.NotEqual(IndexState.Error, replacementIndex.State);
            }
        }
    }
}
