using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes.Errors;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16042 : RavenTestBase
    {
        public RavenDB_16042(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Should_Not_Be_Able_To_Open_Index_Copied_From_Different_Database()
        {
            var options = new Options
            {
                RunInMemory = false,
                ModifyDatabaseRecord = r =>
                {
                    r.Settings[RavenConfiguration.GetKey(x => x.Core.ThrowIfAnyIndexCannotBeOpened)] = "false";
                }
            };

            using (var store1 = GetDocumentStore(options))
            using (var store2 = GetDocumentStore(options))
            {
                await new Products_ByName().ExecuteAsync(store1);
                await new Products_ByName().ExecuteAsync(store2);

                var database1 = await GetDocumentDatabaseInstanceFor(store1);
                var database2 = await GetDocumentDatabaseInstanceFor(store2);

                var indexPath1 = database1.IndexStore.GetIndex(new Products_ByName().IndexName).Configuration.StoragePath.FullPath;
                var indexPath2 = database2.IndexStore.GetIndex(new Products_ByName().IndexName).Configuration.StoragePath.FullPath;

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store1.Database);
                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store2.Database);

                database1 = await GetDocumentDatabaseInstanceFor(store1);

                var index1 = database1.IndexStore.GetIndex(new Products_ByName().IndexName);
                Assert.IsType(typeof(MapIndex), index1);

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store1.Database);

                IOExtensions.DeleteDirectory(indexPath2);
                IOExtensions.MoveDirectory(indexPath1, indexPath2);

                database2 = await GetDocumentDatabaseInstanceFor(store2);

                var index2 = database2.IndexStore.GetIndex(new Products_ByName().IndexName);
                Assert.IsType(typeof(FaultyInMemoryIndex), index2);
            }
        }

        [Fact]
        public async Task Should_Be_Able_To_Use_Soft_Delete_And_Open_Index_Afterwards()
        {
            var path = NewDataPath();

            using (var store = GetDocumentStore(new Options
            {
                RunInMemory = false,
                Path = path
            }))
            {
                await new Products_ByName().ExecuteAsync(store);

                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(store.Database, hardDelete: false));

                await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(new DatabaseRecord(store.Database)
                {
                    Settings =
                    {
                        {RavenConfiguration.GetKey(x => x.Core.RunInMemory), "false" },
                        {RavenConfiguration.GetKey(x => x.Core.DataDirectory), path }
                    }
                }));

                var database = await GetDocumentDatabaseInstanceFor(store);

                var index = database.IndexStore.GetIndex(new Products_ByName().IndexName);
                Assert.IsType(typeof(MapIndex), index);
            }
        }

        private class Products_ByName : AbstractIndexCreationTask<Product>
        {
            public Products_ByName()
            {
                Map = products => from product in products
                                  select new
                                  {
                                      product.Name
                                  };
            }
        }
    }
}
