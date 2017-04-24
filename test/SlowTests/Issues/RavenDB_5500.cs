using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Server.Config;
using SlowTests.Core.Utils.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5500 : RavenTestBase
    {
        [Fact(Skip = "RavenDB-6816")]
        public void WillThrowIfIndexPathIsNotDefinedInDatabaseConfiguration()
        {
            var path = NewDataPath();
            var otherPath = NewDataPath();
            using (var store = GetDocumentStore(path: path))
            {
                var index = new Users_ByCity();
                var indexDefinition = index.CreateIndexDefinition();
                indexDefinition.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StoragePath)] = otherPath;
                indexDefinition.Name = index.IndexName;
                var e = Assert.Throws<RavenException>(() => store.Admin.Send(new PutIndexesOperation(new[] { indexDefinition})));
                Assert.Contains(otherPath, e.Message);
            }
        }

        [Fact(Skip = "RavenDB-6816")]
        public async Task CanCreateInMemoryIndex()
        {
            var index = new Users_ByCity();

            var path = NewDataPath();
            using (var store = GetDocumentStore(path: path))
            {

                var indexDefinition1 = index.CreateIndexDefinition();
                indexDefinition1.Configuration[RavenConfiguration.GetKey(x => x.Indexing.RunInMemory)] = "true";
                indexDefinition1.Name = index.IndexName + "_1";
                store.Admin.Send(new PutIndexesOperation(new[] { indexDefinition1 }));

                var indexDefinition2 = index.CreateIndexDefinition();
                indexDefinition1.Configuration[RavenConfiguration.GetKey(x => x.Indexing.RunInMemory)] = "false";
                indexDefinition2.Name = index.IndexName + "_2";
                store.Admin.Send(new PutIndexesOperation(new[] { indexDefinition2 }));

                var database = await GetDocumentDatabaseInstanceFor(store);

                var directories = Directory.GetDirectories(database.Configuration.Indexing.StoragePath.FullPath);

                Assert.Equal(1, directories.Length); // 1 index
            }

            using (var store = GetDocumentStore(path: path))
            {
                var indexDefinition = store.Admin.Send(new GetIndexOperation(index.IndexName + "_1"));
                Assert.Null(indexDefinition);

                indexDefinition = store.Admin.Send(new GetIndexOperation(index.IndexName + "_2"));
                Assert.NotNull(indexDefinition);
            }
        }
    }
}