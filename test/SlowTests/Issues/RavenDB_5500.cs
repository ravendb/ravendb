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
        [Fact]
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
    }
}