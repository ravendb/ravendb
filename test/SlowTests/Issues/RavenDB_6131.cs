using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6131 : RavenTestBase
    {
        private class SimpleIndex : AbstractIndexCreationTask<Order>
        {
            public SimpleIndex()
            {
                Map = orders => from order in orders
                                select new
                                {
                                    order.Company
                                };
            }
        }

        [Fact]
        public async Task IndexPathsInheritance_DatabaseSpecificSettings()
        {
            var path1 = NewDataPath();
            var path2 = NewDataPath();
            var path3 = NewDataPath();
            var path4 = NewDataPath();

            using (var store = GetDocumentStore(path: path1, modifyDatabaseRecord: document =>
            {
                document.Settings[RavenConfiguration.GetKey(x => x.Indexing.StoragePath)] = path2;
                document.Settings[RavenConfiguration.GetKey(x => x.Indexing.TempPath)] = path3;
                document.Settings[RavenConfiguration.GetKey(x => x.Indexing.JournalsStoragePath)] = path4;
            }))
            {
                var index = new SimpleIndex();
                index.Execute(store);

                var database = await GetDocumentDatabaseInstanceFor(store);
                Assert.Equal(path1, database.Configuration.Core.DataDirectory.FullPath);
                Assert.Equal(path2, database.Configuration.Indexing.StoragePath.FullPath);
                Assert.Equal(path3, database.Configuration.Indexing.TempPath.FullPath);
                Assert.Equal(path4, database.Configuration.Indexing.JournalsStoragePath.FullPath);

                var indexInstance = database.IndexStore.GetIndex(index.IndexName);
                var safeName = IndexDefinitionBase.GetIndexNameSafeForFileSystem(indexInstance.Name);
                var storagePath = Path.Combine(path2, safeName);
                var tempPath = Path.Combine(path3, safeName);
                var journalsStoragePath = Path.Combine(path4, safeName);

                Assert.True(Directory.Exists(storagePath));
                Assert.True(Directory.Exists(tempPath));
                Assert.True(Directory.Exists(journalsStoragePath));

                await store.Admin.SendAsync(new DeleteIndexOperation(index.IndexName));

                Assert.False(Directory.Exists(storagePath));
                Assert.False(Directory.Exists(tempPath));
                Assert.False(Directory.Exists(journalsStoragePath));
            }
        }

        [Fact]
        public async Task IndexPathsInheritance_ServerWideSettings()
        {
            var path1 = NewDataPath();
            var path2 = NewDataPath();
            var path3 = NewDataPath();
            var path4 = NewDataPath();

            DoNotReuseServer(new Dictionary<string, string>
            {
                {RavenConfiguration.GetKey(x => x.Indexing.StoragePath), path2},
                {RavenConfiguration.GetKey(x => x.Indexing.TempPath), path3},
                {RavenConfiguration.GetKey(x => x.Indexing.JournalsStoragePath), path4}
            });

            using (var server = GetNewServer())
            {
                using (var store = GetDocumentStore(path: path1))
                {
                    var index = new SimpleIndex();
                    index.Execute(store);

                    var database = await GetDocumentDatabaseInstanceFor(store);
                    Assert.Equal(path1, database.Configuration.Core.DataDirectory.FullPath);
                    Assert.Equal(Path.Combine(path2, "Databases", store.DefaultDatabase), database.Configuration.Indexing.StoragePath.FullPath);
                    Assert.Equal(Path.Combine(path3, "Databases", store.DefaultDatabase), database.Configuration.Indexing.TempPath.FullPath);
                    Assert.Equal(Path.Combine(path4, "Databases", store.DefaultDatabase), database.Configuration.Indexing.JournalsStoragePath.FullPath);

                    var indexInstance = database.IndexStore.GetIndex(index.IndexName);
                    var safeName = IndexDefinitionBase.GetIndexNameSafeForFileSystem(indexInstance.Name);
                    var storagePath = Path.Combine(path2, "Databases", store.DefaultDatabase, safeName);
                    var tempPath = Path.Combine(path3, "Databases", store.DefaultDatabase, safeName);
                    var journalsStoragePath = Path.Combine(path4, "Databases", store.DefaultDatabase, safeName);

                    Assert.True(Directory.Exists(storagePath));
                    Assert.True(Directory.Exists(tempPath));
                    Assert.True(Directory.Exists(journalsStoragePath));

                    await store.Admin.SendAsync(new DeleteIndexOperation(index.IndexName));

                    Assert.False(Directory.Exists(storagePath));
                    Assert.False(Directory.Exists(tempPath));
                    Assert.False(Directory.Exists(journalsStoragePath));
                }
            }
        }
    }
}