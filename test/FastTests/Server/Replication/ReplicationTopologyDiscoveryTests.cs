using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Documents.Replication;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Http;
using Sparrow.Json;
using Xunit;

namespace FastTests.Server.Replication
{
    public class ReplicationTopologyDiscoveryTests : ReplicationTestsBase
    {
        [Fact]
        public async Task Empty_topology_should_be_correctly_detected()
        {
            using (var store = GetDocumentStore())
            {
                var topology = await GetTopology(store);
                Assert.Empty(topology.Outgoing);                
                Assert.NotNull(topology.LeaderNode);
            }
        }

        [Fact]
        public async Task Master_slave_topology_should_be_correctly_detected()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                SetupReplication(master,slave);
                var topology = await GetTopology(master);
                Assert.Contains(slave.DefaultDatabase, topology.Outgoing.Select(x => x.Node.Database));
            }
        }

        private static async Task<Topology> GetTopology(Raven.Client.Document.DocumentStore store)
        {
            using (var executer = new RequestExecuter(store.Url, store.DefaultDatabase, null))
            using (var context = new JsonOperationContext(1024 * 1024, 1024 * 1024))
            {
                var getTopologyCommand = new GetTopologyCommand();
                await executer.ExecuteAsync(getTopologyCommand, context);

                return getTopologyCommand.Result;
            }
        }
    }
}
