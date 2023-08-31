using System.Threading.Tasks;
using SlowTests.Core.AdminConsole;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests
{
    public class RavenDB_18634 : ClusterTestBase
    {
        public RavenDB_18634(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task DisableTcpCompressionIn1ServerOutOf2InCluster(bool watcherCluster)
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: watcherCluster);

            // modify configuration
            AdminJsConsoleTests.ExecuteScript(leader, database: null, "server.Configuration.Server.DisableTcpCompression = true;");
            Assert.True(leader.Configuration.Server.DisableTcpCompression);

            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2});

            var db0 = await nodes[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            var db1 = await nodes[1].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            Assert.NotNull(db0);
            Assert.NotNull(db1);
        }
    }
}
