using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Operations.Databases.Indexes;
using Raven.Server.Config;
using Raven.Server.Documents;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6131 : RavenNewTestBase
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
        public async Task IndexPathsInheritance()
        {
            var path1 = NewDataPath();
            var path2 = NewDataPath();
            var path3 = NewDataPath();
            var path4 = NewDataPath();

            using (var store = GetDocumentStore(path: path1, modifyDatabaseDocument: document =>
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
                var safeName = indexInstance.GetIndexNameSafeForFileSystem();
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
    }
}