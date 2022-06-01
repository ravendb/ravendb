using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax
{
    public class IndexBackwardCompatibilityAndPersistence : RavenTestBase
    {
        private readonly string _serverPath;
        private readonly string _databasePath;
        private string _indexStoragePath1;
        private string _indexStoragePath2;
        private string _databaseName;
        private readonly Orders_ByOrderBy _index;

        public IndexBackwardCompatibilityAndPersistence(ITestOutputHelper output) : base(output)
        {
            _serverPath = NewDataPath();
            _databasePath = NewDataPath();
            _databaseName = string.Empty;
            _indexStoragePath1 = string.Empty;
            _indexStoragePath2 = string.Empty;
            _databaseName = string.Empty;
            _index = new();
        }

        [Theory]
        [InlineData(SearchEngineType.Corax, SearchEngineType.Lucene)]
        [InlineData(SearchEngineType.Lucene, SearchEngineType.Corax)]
        public async Task AutoPersistIndexConfiguration(SearchEngineType beginType, SearchEngineType endType)
        {
            // using (var server = GetNewServer(new ServerCreationOptions { DataDirectory = NewDataPath(), RunInMemory = false }))
            // {
                using (var store = GetDocumentStore(new Options {RunInMemory = false, Path = NewDataPath(), ModifyDatabaseRecord = d => d.Settings[RavenConfiguration.GetKey(x => x.Indexing.AutoIndexingEngineType)] = beginType.ToString() }))
                {
                    _databaseName = store.Database;
                    using (var session = store.OpenSession())
                        _ = session.Query<Order>().Where(x => x.OrderedAt == DateTime.Now).ToList();
                    var database = await GetDatabase(store.Database); //await store.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    var index = database.IndexStore.GetIndex("Auto/Orders/ByOrderedAt");

                    Assert.Equal(beginType, index.SearchEngineType);
                    PutConfigurationSettings(store, "Indexing.Auto.SearchEngine", endType);
                    var index2 = database.IndexStore.GetIndex("Auto/Orders/ByOrderedAt");

                    Assert.Equal(beginType, index2.SearchEngineType);
                }
           // }
        }

        //todo maciej: There will be tests for StaticIndexes
        [Fact]
        public async Task LoadOldIndexWithoutRebuildingForNewIndexingEngine()
        {
            using (var server = GetNewServer(new ServerCreationOptions { DataDirectory = _serverPath, RunInMemory = false }))
            using (var store = GetDocumentStore(new Options { Server = server, RunInMemory = false, Path = _databasePath }))
            {
                _databaseName = store.Database;
                _index.Execute(store);
                using (var session = store.OpenSession())
                {
                    var orders = session.Query<Order>()
                        .Where(x => x.OrderedAt == DateTime.Now).ToList();
                }

                Indexes.WaitForIndexing(store);
                WaitForUserToContinueTheTest(store);

                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                _indexStoragePath1 = database.IndexStore.GetIndex(_index.IndexName)._environment.Options.BasePath.FullPath;
                _indexStoragePath2 = database.IndexStore.GetIndex("Auto/Orders/ByOrderedAt")._environment.Options.BasePath.FullPath;

                Assert.NotNull(_indexStoragePath1);
                Assert.NotNull(_indexStoragePath2);
            }

            DeleteAndExtractFiles(_indexStoragePath2, "Precorax.Orders_ByOrderBy.zip");
            DeleteAndExtractFiles(_indexStoragePath1, "Precorax.Auto_Orders_ByOrderedAt.zip");

            using (var server = GetNewServer(new ServerCreationOptions { DataDirectory = _serverPath, RunInMemory = false }))
            using (var store = GetDocumentStore(new Options { Server = server, RunInMemory = false, Path = _databasePath, ModifyDatabaseName = _ => _databaseName }))
            {
                var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                Indexes.WaitForIndexing(store);

                var indexInstance1 = database.IndexStore.GetIndex(_index.IndexName);
                var indexInstance2 = database.IndexStore.GetIndex("Auto/Orders/ByOrderedAt");

                Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.BaseVersion, indexInstance1.Definition.Version);
                Assert.Equal(IndexDefinitionBaseServerSide.IndexVersion.BaseVersion, indexInstance2.Definition.Version);

                Assert.Equal(IndexState.Normal, indexInstance1.State);
                Assert.Equal(IndexState.Normal, indexInstance2.State);

                Assert.Equal(SearchEngineType.Lucene, indexInstance1.SearchEngineType);
                Assert.Equal(SearchEngineType.Lucene, indexInstance2.SearchEngineType);
            }
        }

        private void PutConfigurationSettings(DocumentStore store, string key, SearchEngineType changedTo)
        {
            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
            try
            {
                var settings = new Dictionary<string, string>() { [key] = changedTo.ToString() };
                store.Maintenance.Send(new PutDatabaseSettingsOperation(store.Database, settings));
                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));
                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));
            }
            catch(ConcurrencyException)
            {
                var recordJson = JsonConvert.SerializeObject(record);
                Console.WriteLine($"Original: {recordJson}");
                var record2 = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(store.Database));
                var recordJson2 = JsonConvert.SerializeObject(record2);
                Console.WriteLine($"\nModified: {recordJson2}");

                throw;
            }
        }

        private static void DeleteAndExtractFiles(string destination, string zipPathInAssembly)
        {
            using (var stream1 = GetFile(zipPathInAssembly))
            using (var archive1 = new ZipArchive(stream1))
            {
                IOExtensions.DeleteDirectory(destination);
                archive1.ExtractToDirectory(destination);
            }
        }

        private class Orders_ByOrderBy : AbstractIndexCreationTask<Order>
        {
            public Orders_ByOrderBy()
            {
                Map = orders => from o in orders
                    select new { o.OrderedAt, o.ShippedAt };
            }
        }

        private static Stream GetFile(string name)
        {
            var assembly = typeof(IndexBackwardCompatibilityAndPersistence).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_17070." + name);
        }
    }
}
