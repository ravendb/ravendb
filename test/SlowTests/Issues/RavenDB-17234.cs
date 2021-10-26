using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17234 : ClusterTestBase
    {
        public RavenDB_17234(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanTargetOperationsAgainstWatcherNodeUsingDocumentStoreServerOperationsExecutor()
        {
            var databaseName = GetDatabaseName();
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);
            var clusterTopology = leader.ServerStore.GetClusterTopology();

            Assert.Equal(2, clusterTopology.AllNodes.Count);
            Assert.Equal(1, clusterTopology.Members.Count);
            Assert.Equal(1, clusterTopology.Watchers.Count);

            await CreateDatabaseInCluster(databaseName, 2, leader.WebUrl);

            using (var store = new DocumentStore
            {
                Urls = nodes.Select(n => n.WebUrl).ToArray(), 
                Database = databaseName
            }.Initialize())
            {
                var watcherTag = clusterTopology.Watchers.Single().Key;
                var rec = await store.Maintenance.Server.ForNode(watcherTag).SendAsync(new GetDatabaseRecordOperation(databaseName));
                Assert.NotNull(rec);
                Assert.Equal(databaseName, rec.DatabaseName);
            }
        }
    }
}
