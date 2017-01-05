using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests.Server.Documents.Replication;
using Raven.Abstractions.Connection;
using Raven.Client.Document;
using Raven.Client.Replication.Messages;
using Xunit;
using Raven.Client.Connection;
using Raven.Server.Config.Settings;

namespace FastTests.Server.Replication
{
    public class ReplicationTopologyDiscoveryTests : ReplicationTestsBase
    {
        [Fact]
        public async Task Without_replication_full_topology_should_return_empty_topology_info()
        {
            using (var store = GetDocumentStore())
            {
                var storeDocumentDatabase = await GetDocumentDatabaseInstanceFor(store);
                var topologyInfo = await GetFullTopology(store);

                Assert.NotNull(topologyInfo); //sanity check
                Assert.Empty(topologyInfo.NodesByDbId);
                Assert.Equal(storeDocumentDatabase.DbId.ToString(),topologyInfo.LeaderDbId);
            }
        }

        [Fact]
        public async Task Master_slave_full_topology_should_be_correctly_detected()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                var masterDocumentDatabase = await GetDocumentDatabaseInstanceFor(master);
                var slaveDocumentDatabase = await GetDocumentDatabaseInstanceFor(slave);

                SetupReplication(master,slave);

                var topologyInfo = await GetFullTopology(master);

                Assert.NotNull(topologyInfo); //sanity check

                //total two nodes, master and slave
                Assert.Equal(2, topologyInfo.NodesByDbId.Count);

                //one outgoing node at the master
                Assert.Equal(1, topologyInfo.NodesByDbId[masterDocumentDatabase.DbId.ToString()].Outgoing.Count);
                Assert.Equal(slaveDocumentDatabase.DbId.ToString(),
                    topologyInfo.NodesByDbId[masterDocumentDatabase.DbId.ToString()].Outgoing.First().DbId);
            }
        }

        [Fact]
        public async Task Master_two_slaves_full_topology_should_be_correctly_detected()
        {
            using (var master = GetDocumentStore())
            using (var slave1 = GetDocumentStore())
            using (var slave2 = GetDocumentStore())
            {
                var masterDocumentDatabase = await GetDocumentDatabaseInstanceFor(master);
                var slave1DocumentDatabase = await GetDocumentDatabaseInstanceFor(slave1);
                var slave2DocumentDatabase = await GetDocumentDatabaseInstanceFor(slave2);

                SetupReplication(master, slave1, slave2);

                var topologyInfo = await GetFullTopology(master);

                Assert.NotNull(topologyInfo); //sanity check
                Assert.Equal(3, topologyInfo.NodesByDbId.Count);
                var masterOutgoing = topologyInfo.NodesByDbId[masterDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(2, masterOutgoing.Count);
                Assert.True(masterOutgoing.Select(x => x.DbId).Any(x => x == slave1DocumentDatabase.DbId.ToString()));
                Assert.True(masterOutgoing.Select(x => x.DbId).Any(x => x == slave2DocumentDatabase.DbId.ToString()));
            }
        }

        [Fact]
        public async Task Master_master_full_topology_should_be_correctly_detected()
        {
            using (var nodeA = GetDocumentStore())
            using (var nodeB = GetDocumentStore())
            {
                //add some documents so incoming replication handlers will be initialized
                using (var session = nodeA.OpenSession())
                {
                    session.Store(new { Foo = "Bar"},"users/1");
                    session.SaveChanges();
                }
                using (var session = nodeB.OpenSession())
                {
                    session.Store(new { Foo = "Bar2" }, "users/2");
                    session.SaveChanges();
                }

                SetupReplication(nodeA,nodeB);
                SetupReplication(nodeB,nodeA);

                WaitForDocument(nodeA, "users/2");
                WaitForDocument(nodeB, "users/1");

                var nodeADocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeA);
                var nodeBDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeB);

                var topologyInfo = await GetFullTopology(nodeA);
                Assert.Equal(2,topologyInfo.NodesByDbId.Count);

                var nodeAOutgoing = topologyInfo.NodesByDbId[nodeADocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1,nodeAOutgoing.Count);
                Assert.Equal(nodeBDocumentDatabase.DbId.ToString(), nodeAOutgoing.First().DbId);
                var nodeAIncoming = topologyInfo.NodesByDbId[nodeADocumentDatabase.DbId.ToString()].Incoming;
                Assert.Equal(1, nodeAIncoming.Count);
                Assert.Equal(nodeBDocumentDatabase.DbId.ToString(), nodeAIncoming.First().DbId);

                var nodeBOutgoing = topologyInfo.NodesByDbId[nodeBDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, nodeBOutgoing.Count);
                Assert.Equal(nodeADocumentDatabase.DbId.ToString(), nodeBOutgoing.First().DbId);
                var nodeBIncoming = topologyInfo.NodesByDbId[nodeBDocumentDatabase.DbId.ToString()].Incoming;
                Assert.Equal(1, nodeBIncoming.Count);
                Assert.Equal(nodeADocumentDatabase.DbId.ToString(), nodeBIncoming.First().DbId);

            }
        }

        [Fact]
        public async Task Master_master_master_full_topology_should_be_correctly_detected()
        {
            using (var nodeA = GetDocumentStore())
            using (var nodeB = GetDocumentStore())
            using (var nodeC = GetDocumentStore())
            {
                SetupReplication(nodeA, nodeB);
                SetupReplication(nodeB, nodeC);
                SetupReplication(nodeC, nodeA);

                var nodeADocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeA);
                var nodeBDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeB);
                var nodeCDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeC);

                var topologyInfo = await GetFullTopology(nodeA);
                Assert.Equal(3, topologyInfo.NodesByDbId.Count);

                var nodeAOutgoing = topologyInfo.NodesByDbId[nodeADocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, nodeAOutgoing.Count);
                Assert.Equal(nodeBDocumentDatabase.DbId.ToString(), nodeAOutgoing.First().DbId);

                var nodeBOutgoing = topologyInfo.NodesByDbId[nodeBDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, nodeBOutgoing.Count);
                Assert.Equal(nodeCDocumentDatabase.DbId.ToString(), nodeBOutgoing.First().DbId);

                var nodeCOutgoing = topologyInfo.NodesByDbId[nodeCDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, nodeCOutgoing.Count);
                Assert.Equal(nodeADocumentDatabase.DbId.ToString(), nodeCOutgoing.First().DbId);
            }
        }

        [Fact]
        public async Task Cluster_with_master_master_master_and_some_leaves_full_topology_should_be_correctly_detected()
        {
            using (var nodeA = GetDocumentStore())
            using (var nodeB = GetDocumentStore())
            using (var nodeC = GetDocumentStore())
            using (var nodeALeaf = GetDocumentStore())
            using (var nodeCLeaf = GetDocumentStore())
            {
                SetupReplication(nodeA, nodeB, nodeALeaf);
                SetupReplication(nodeB, nodeC);
                SetupReplication(nodeC, nodeA, nodeCLeaf);

                var nodeADocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeA);
                var nodeALeafDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeALeaf);
                var nodeBDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeB);
                var nodeCDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeC);
                var nodeCLeafDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeCLeaf);

                var topologyInfo = await GetFullTopology(nodeA);
                Assert.Equal(5, topologyInfo.NodesByDbId.Count);

                var nodeAOutgoing = topologyInfo.NodesByDbId[nodeADocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(2, nodeAOutgoing.Count);
                Assert.True(nodeAOutgoing.Select(x => x.DbId).Any(x => x == nodeBDocumentDatabase.DbId.ToString()));
                Assert.True(nodeAOutgoing.Select(x => x.DbId).Any(x => x == nodeALeafDocumentDatabase.DbId.ToString()));

                var nodeBOutgoing = topologyInfo.NodesByDbId[nodeBDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, nodeBOutgoing.Count);
                Assert.Equal(nodeCDocumentDatabase.DbId.ToString(), nodeBOutgoing.First().DbId);

                var nodeCOutgoing = topologyInfo.NodesByDbId[nodeCDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(2, nodeCOutgoing.Count);
                Assert.True(nodeCOutgoing.Select(x => x.DbId).Any(x => x == nodeADocumentDatabase.DbId.ToString()));
                Assert.True(nodeCOutgoing.Select(x => x.DbId).Any(x => x == nodeCLeafDocumentDatabase.DbId.ToString()));
            }
        }

        [Fact]
        public async Task Master_slave_slaveOfslave_full_topology_should_be_correctly_detected()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            using (var slaveOfSlave = GetDocumentStore())
            {
                SetupReplication(master, slave);
                SetupReplication(slave, slaveOfSlave);

                var masterDocumentDatabase = await GetDocumentDatabaseInstanceFor(master);
                masterDocumentDatabase.Configuration.Replication.ReplicationTopologyDiscoveryTimeout = new TimeSetting(24,TimeUnit.Hours);
                var slaveDocumentDatabase = await GetDocumentDatabaseInstanceFor(slave);
                slaveDocumentDatabase.Configuration.Replication.ReplicationTopologyDiscoveryTimeout = new TimeSetting(24, TimeUnit.Hours);
                var slaveOfSlaveDocumentDatabase = await GetDocumentDatabaseInstanceFor(slaveOfSlave);
                slaveOfSlaveDocumentDatabase.Configuration.Replication.ReplicationTopologyDiscoveryTimeout = new TimeSetting(24, TimeUnit.Hours);

                var topologyInfo = await GetFullTopology(master);
                Assert.Equal(3, topologyInfo.NodesByDbId.Count);
                var masterNodeOutgoing = topologyInfo.NodesByDbId[masterDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1,masterNodeOutgoing.Count);

                Assert.Equal(slaveDocumentDatabase.DbId.ToString(), masterNodeOutgoing.First().DbId);

                var slaveNodeOutgoing = topologyInfo.NodesByDbId[slaveDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, slaveNodeOutgoing.Count);

                Assert.Equal(slaveOfSlaveDocumentDatabase.DbId.ToString(), slaveNodeOutgoing.First().DbId);
            }
        }

        private async Task<FullTopologyInfo> GetFullTopology(DocumentStore store)
        {
            var url = $"{store.Url}/databases/{store.DefaultDatabase}/topology/full";
            using (var request = store.JsonRequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(null, url, 
                    HttpMethod.Get, 
                    new OperationCredentials(null, CredentialCache.DefaultCredentials), 
                    new DocumentConvention())))
            {
                var topologyInfoJson = await request.ReadResponseJsonAsync();
                return topologyInfoJson.ToObject<FullTopologyInfo>();
            }

        }
    }
}
