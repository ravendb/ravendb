using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.LowMemory;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12269 : RavenTestBase
    {
        public RavenDB_12269(ITestOutputHelper output) : base(output)
        {
        }

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

                Indexes.WaitForIndexing(store);

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

                
                var replacementIndex = db.IndexStore.GetIndex($"{Constants.Documents.Indexing.SideBySideIndexNamePrefix}Users_ByName");

                // let's try to force calling storageEnvironment.Cleanup() inside ExecuteIndexing method
                replacementIndex.LowMemory(LowMemorySeverity.ExtremelyLow);
                var envOfReplacementIndex = replacementIndex._indexStorage.Environment();
                envOfReplacementIndex.LogsApplied();

                db.IndexStore.StartIndexing();

                Indexes.WaitForIndexing(store);

                // let's try to force calling storageEnvironment.Cleanup() inside ExecuteIndexing method
                replacementIndex.LowMemory(LowMemorySeverity.ExtremelyLow);
                envOfReplacementIndex.LogsApplied();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Foo" });

                    await session.SaveChangesAsync();

                    // this will ensure that index isn't in error state
                    var count = await session.Query<User>("Users_ByName").Customize(x => x.WaitForNonStaleResults()).CountAsync();
                    Assert.Equal(3, count);
                }
            }
        }
    }
}
