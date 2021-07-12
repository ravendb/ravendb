using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Indexes;
using Raven.Server.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13759 : RavenTestBase
    {
        public RavenDB_13759(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanStoreTimeInformationInIndexStorageAndRecoverThatAfterReloadingDatabase()
        {
            var path = NewDataPath();

            using (var store = GetDocumentStore(new Options { Path = path }))
            {
                var index = new Orders_ByOrderBy();
                index.Execute(store);

                var database = await GetDocumentDatabaseInstanceFor(store);
                var indexInstance1 = database.IndexStore.GetIndex(index.IndexName);

                Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion, indexInstance1.Definition.Version);

                WaitForIndexing(store);

                var indexTimeFields = indexInstance1._indexStorage.ReadIndexTimeFields();
                Assert.Empty(indexTimeFields);

                var now1 = DateTime.Now;

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { OrderedAt = now1, ShippedAt = null }, "orders/1");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                indexTimeFields = indexInstance1._indexStorage.ReadIndexTimeFields();
                Assert.Equal(1, indexTimeFields.Count);
                Assert.Contains("OrderedAt", indexTimeFields);

                var now2 = now1.AddDays(1);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { OrderedAt = now2, ShippedAt = now2 }, "orders/2");
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                indexTimeFields = indexInstance1._indexStorage.ReadIndexTimeFields();
                Assert.Equal(2, indexTimeFields.Count);
                Assert.Contains("OrderedAt", indexTimeFields);
                Assert.Contains("ShippedAt", indexTimeFields);

                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
                database = await GetDocumentDatabaseInstanceFor(store);

                var indexInstance2 = database.IndexStore.GetIndex(index.IndexName);
                Assert.NotEqual(indexInstance1, indexInstance2);

                Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion, indexInstance2.Definition.Version);

                indexTimeFields = indexInstance2._indexStorage.ReadIndexTimeFields();
                Assert.Equal(2, indexTimeFields.Count);
                Assert.Contains("OrderedAt", indexTimeFields);
                Assert.Contains("ShippedAt", indexTimeFields);
            }
        }

        [Fact]
        public async Task CanOpenIndexesWithOlderVersion()
        {
            var serverPath = NewDataPath();
            var databasePath = NewDataPath();
            string indexStoragePath1, indexStoragePath2;
            string databaseName;

            var index = new Orders_ByOrderBy();

            using (var server = GetNewServer(new ServerCreationOptions { DataDirectory = serverPath, RunInMemory = false }))
            using (var store = GetDocumentStore(new Options { Server = server, RunInMemory = false, Path = databasePath }))
            {
                databaseName = store.Database;
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    var orders = session.Query<Order>()
                        .Where(x => x.OrderedAt >= DateTime.Now)
                        .ToList();
                }

                WaitForIndexing(store);

                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                indexStoragePath1 = database.IndexStore.GetIndex(index.IndexName)._environment.Options.BasePath.FullPath;
                indexStoragePath2 = database.IndexStore.GetIndex("Auto/Orders/ByOrderedAt")._environment.Options.BasePath.FullPath;

                Assert.NotNull(indexStoragePath1);
                Assert.NotNull(indexStoragePath2);
            }

            using (var stream1 = GetFile("Orders_ByOrderBy.zip"))
            using (var stream2 = GetFile("Auto_Orders_ByOrderedAt.zip"))
            using (var archive1 = new ZipArchive(stream1))
            using (var archive2 = new ZipArchive(stream2))
            {
                IOExtensions.DeleteDirectory(indexStoragePath1);
                IOExtensions.DeleteDirectory(indexStoragePath2);

                archive1.ExtractToDirectory(indexStoragePath1);
                archive2.ExtractToDirectory(indexStoragePath2);
            }

            using (var server = GetNewServer(new ServerCreationOptions { DataDirectory = serverPath, RunInMemory = false }))
            using (var store = GetDocumentStore(new Options { Server = server, RunInMemory = false, Path = databasePath, ModifyDatabaseName = _ => databaseName }))
            {
                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                WaitForIndexing(store);

                var indexInstance1 = database.IndexStore.GetIndex(index.IndexName);
                var indexInstance2 = database.IndexStore.GetIndex("Auto/Orders/ByOrderedAt");

                Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.BaseVersion, indexInstance1.Definition.Version);
                Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.BaseVersion, indexInstance2.Definition.Version);

                var indexTimeFields = indexInstance1._indexStorage.ReadIndexTimeFields();
                Assert.Equal(0, indexTimeFields.Count);

                indexTimeFields = indexInstance2._indexStorage.ReadIndexTimeFields();
                Assert.Equal(0, indexTimeFields.Count);

                server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);
                database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                indexInstance1 = database.IndexStore.GetIndex(index.IndexName);
                indexInstance2 = database.IndexStore.GetIndex("Auto/Orders/ByOrderedAt");

                Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.BaseVersion, indexInstance1.Definition.Version);
                Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.BaseVersion, indexInstance2.Definition.Version);

                store.Maintenance.Send(new ResetIndexOperation(index.IndexName));
                store.Maintenance.Send(new ResetIndexOperation("Auto/Orders/ByOrderedAt"));

                indexInstance1 = database.IndexStore.GetIndex(index.IndexName);
                indexInstance2 = database.IndexStore.GetIndex("Auto/Orders/ByOrderedAt");

                Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion, indexInstance1.Definition.Version);
                Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.CurrentVersion, indexInstance2.Definition.Version);
            }
        }

        [Fact]
        public void WhenUsingExactOnDateTimeOffsetWeShouldBeAbleToQueryByThisValue()
        {
            using (var store = GetDocumentStore())
            {
                new Orders_ByOrderBy_DateTimeOffset().Execute(store);

                var now = DateTime.Now;
                var offsetNow = DateTimeOffset.Now;

                using (var session = store.OpenSession())
                {
                    session.Store(new Order_DateTimeOffset { OrderedAt = now, ShippedAt = offsetNow });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession(new SessionOptions { NoCaching = true }))
                {
                    var orders = session.Query<Order_DateTimeOffset, Orders_ByOrderBy_DateTimeOffset>()
                        .Where(x => x.ShippedAt == offsetNow, exact: true)
                        .ToList();

                    Assert.Equal(1, orders.Count);
                }
            }
        }

        private class Orders_ByOrderBy : AbstractIndexCreationTask<Order>
        {
            public Orders_ByOrderBy()
            {
                Map = orders => from o in orders
                                select new
                                {
                                    o.OrderedAt,
                                    o.ShippedAt
                                };
            }
        }

        private class Orders_ByOrderBy_DateTimeOffset : AbstractIndexCreationTask<Order_DateTimeOffset>
        {
            public Orders_ByOrderBy_DateTimeOffset()
            {
                Map = orders => from o in orders
                                select new
                                {
                                    o.OrderedAt,
                                    o.ShippedAt
                                };

                Index(x => x.ShippedAt, FieldIndexing.Exact);
            }
        }

        private class Order_DateTimeOffset
        {
            public DateTime OrderedAt { get; set; }

            public DateTimeOffset? ShippedAt { get; set; }
        }

        private static Stream GetFile(string name)
        {
            var assembly = typeof(RavenDB_13759).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_13759." + name);
        }
    }
}
