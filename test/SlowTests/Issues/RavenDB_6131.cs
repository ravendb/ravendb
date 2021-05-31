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
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6131 : RavenTestBase
    {
        public RavenDB_6131(ITestOutputHelper output) : base(output)
        {
        }

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
            var path3 = NewDataPath();

            using (var store = GetDocumentStore(new Options
            {
                Path = path1,
                ModifyDatabaseRecord = document =>
                {
                    document.Settings[RavenConfiguration.GetKey(x => x.Indexing.TempPath)] = path3;
                }
            }))
            {
                var index = new SimpleIndex();
                index.Execute(store);

                var database = await GetDocumentDatabaseInstanceFor(store);
                Assert.Equal(path1, database.Configuration.Core.DataDirectory.FullPath);
                Assert.Equal(path3, database.Configuration.Indexing.TempPath.FullPath);

                var indexInstance = database.IndexStore.GetIndex(index.IndexName);
                var safeName = IndexDefinitionBase.GetIndexNameSafeForFileSystem(indexInstance.Name);
                var tempPath = Path.Combine(path3, safeName);

                Assert.True(Directory.Exists(tempPath));

                await store.Maintenance.SendAsync(new DeleteIndexOperation(index.IndexName));

                Assert.False(Directory.Exists(tempPath));
            }
        }

        [Fact]
        public async Task IndexPathsInheritance_ServerWideSettings()
        {
            var path1 = NewDataPath();
            var path3 = NewDataPath();

            DoNotReuseServer(new Dictionary<string, string>
            {
                {RavenConfiguration.GetKey(x => x.Indexing.TempPath), path3},
            });

            using (GetNewServer())
            {
                using (var store = GetDocumentStore(new Options
                {
                    Path = path1
                }))
                {
                    var index = new SimpleIndex();
                    index.Execute(store);

                    var database = await GetDocumentDatabaseInstanceFor(store);
                    Assert.Equal(path1, database.Configuration.Core.DataDirectory.FullPath);
                    Assert.Equal(Path.Combine(path3, "Databases", store.Database), database.Configuration.Indexing.TempPath.FullPath);

                    var indexInstance = database.IndexStore.GetIndex(index.IndexName);
                    var safeName = IndexDefinitionBase.GetIndexNameSafeForFileSystem(indexInstance.Name);
                    var tempPath = Path.Combine(path3, "Databases", store.Database, safeName);

                    Assert.True(Directory.Exists(tempPath));

                    await store.Maintenance.SendAsync(new DeleteIndexOperation(index.IndexName));

                    Assert.False(Directory.Exists(tempPath));
                }
            }
        }
    }
}
