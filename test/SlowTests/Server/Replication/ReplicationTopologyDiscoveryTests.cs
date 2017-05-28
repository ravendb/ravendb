using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Server.Config.Settings;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ReplicationTopologyDiscoveryTests : ReplicationTestsBase
    {
        [Fact]
        public async Task Without_replication_live_topology_should_return_empty_topology_info()
        {
            using (var store = GetDocumentStore())
            {
                var storeDocumentDatabase = await GetDocumentDatabaseInstanceFor(store);
                var topologyInfo = GetLiveTopology(store);

                Assert.NotNull(topologyInfo); //sanity check
                Assert.Empty(topologyInfo.NodesById);
                Assert.Equal(storeDocumentDatabase.DbId.ToString(), topologyInfo.DatabaseId);
            }
        }

        [Fact]
        public async Task Master_slave_live_topology_should_be_correctly_detected()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                var masterDocumentDatabase = await GetDocumentDatabaseInstanceFor(master);
                var slaveDocumentDatabase = await GetDocumentDatabaseInstanceFor(slave);

                await SetupReplicationAsync(master, slave);
                EnsureReplicating(master, slave);
                var topologyInfo = GetLiveTopology(master);

                Assert.NotNull(topologyInfo); //sanity check

                //total two nodes, master and slave
                Assert.Equal(2, topologyInfo.NodesById.Count);

                //one outgoing node at the master
                Assert.Equal(1, topologyInfo.NodesById[masterDocumentDatabase.DbId.ToString()].Outgoing.Count);
                Assert.Equal(slaveDocumentDatabase.DbId.ToString(),
                    topologyInfo.NodesById[masterDocumentDatabase.DbId.ToString()].Outgoing.First().DbId);
            }
        }

        [Fact]
        public async Task Master_slave_that_goes_offline_with_live_topology_that_should_be_correctly_detected()
        {
            DocumentStore slave = null;
            try
            {
                slave = GetDocumentStore();
                using (var master = GetDocumentStore())
                {
                    var masterDocumentDatabase = await GetDocumentDatabaseInstanceFor(master);

                    await SetupReplicationAsync(master, slave);
                    EnsureReplicating(master, slave);
                    var topologyInfo = GetLiveTopology(master);

                    Assert.NotNull(topologyInfo); //sanity check

                    //one outgoing node at the master
                    Assert.Equal(1, topologyInfo.NodesById[masterDocumentDatabase.DbId.ToString()].Outgoing.Count);

                    //now it goes offline
                    slave.Dispose();
                    slave = null;

                    //add document to master to force outgoing handler to "notice" that remote node is offline
                    using (var session = master.OpenSession())
                    {
                        session.Store(new { Foo = "Bar" }, "foo/bar");
                        session.SaveChanges();
                    }

                    topologyInfo = GetLiveTopology(master);
                    Assert.Equal(0, topologyInfo.NodesById[masterDocumentDatabase.DbId.ToString()].Outgoing.Count);
                    Assert.Equal(1, topologyInfo.NodesById[masterDocumentDatabase.DbId.ToString()].Offline.Count);

                var destination = masterDocumentDatabase.ReplicationLoader.Destinations.ToList();
                    Assert.Equal(1, destination.Count); //sanity check, this should always be true
                    var slaveUrl = destination.First().Url;
                    var slaveDatabase = destination.First().Database;

                    var offlineNodeInfo = topologyInfo.NodesById[masterDocumentDatabase.DbId.ToString()].Offline.First();

                    Assert.Equal(slaveUrl, offlineNodeInfo.Url);
                    Assert.Equal(slaveDatabase, offlineNodeInfo.Database);
                }
            }
            finally
            {
                slave?.Dispose();
            }
        }

        [Fact]
        public async Task Master_slave_live_topology_incoming_nodes_should_be_correctly_detected()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                var masterDocumentDatabase = await GetDocumentDatabaseInstanceFor(master);
                var slaveDocumentDatabase = await GetDocumentDatabaseInstanceFor(slave);

                await SetupReplicationAsync(master, slave);
                EnsureReplicating(master, slave);
                var topologyInfo = GetLiveTopology(master);

                Assert.NotNull(topologyInfo); //sanity check

                //total two nodes, master and slave
                Assert.Equal(2, topologyInfo.NodesById.Count);

                Assert.Equal(1, topologyInfo.NodesById[slaveDocumentDatabase.DbId.ToString()].Incoming.Count);
                Assert.Equal(masterDocumentDatabase.DbId.ToString(),
                    topologyInfo.NodesById[slaveDocumentDatabase.DbId.ToString()].Incoming.First().DbId);
            }
        }

        /*
         *  topology here:
         *  
         *    --> (B) --> 
         *   ^           |
         *   |           V
         *  (A)         (D)
         *   |           ^
         *   V           |
         *    --> (C) --> 
         */
        [Fact]
        public async Task Master_slave_two_tiers_with_live_topology_incoming_should_be_correctly_detected()
        {
            using (var A = GetDocumentStore())
            using (var B = GetDocumentStore())
            using (var C = GetDocumentStore())
            using (var D = GetDocumentStore())
            {
                var BDocumentDatabase = await GetDocumentDatabaseInstanceFor(B);
                var CDocumentDatabase = await GetDocumentDatabaseInstanceFor(C);
                var DDocumentDatabase = await GetDocumentDatabaseInstanceFor(D);

                await SetupReplicationAsync(A, B, C);
                EnsureReplicating(A, B);
                EnsureReplicating(A, C);
                await SetupReplicationAsync(B, D);
                EnsureReplicating(B, D);
                await SetupReplicationAsync(C, D);
                EnsureReplicating(C, D);

                var topologyInfo = GetLiveTopology(A);

                Assert.NotNull(topologyInfo); //sanity check

                //total two nodes, master and slave
                Assert.Equal(4, topologyInfo.NodesById.Count);

                Assert.Equal(0, topologyInfo.NodesById[DDocumentDatabase.DbId.ToString()].Outgoing.Count);
                var incomingOfD = topologyInfo.NodesById[DDocumentDatabase.DbId.ToString()].Incoming;
                Assert.Equal(2, incomingOfD.Count);
                Assert.True(incomingOfD.Select(x => x.DbId).Any(x => x == BDocumentDatabase.DbId.ToString()));
                Assert.True(incomingOfD.Select(x => x.DbId).Any(x => x == CDocumentDatabase.DbId.ToString()));
            }
        }

        // (A) --> (B) --> (C)
        [Fact]
        public async Task Master_slave_two_tiers_with_live_topology_with_nodes_that_goes_offline_they_should_be_correctly_detected()
        {
            DocumentStore B = null;
            try
            {
                B = GetDocumentStore();
                using (var A = GetDocumentStore())
                using (var C = GetDocumentStore())
                {
                    var ADocumentDatabase = await GetDocumentDatabaseInstanceFor(A);
                    var BDocumentDatabase = await GetDocumentDatabaseInstanceFor(B);

                    await SetupReplicationAsync(A, B);
                    EnsureReplicating(A, B);
                    await SetupReplicationAsync(B, C);
                    EnsureReplicating(B, C);

                    var topologyInfo = GetLiveTopology(A);
                    //total two nodes, master and slave
                    Assert.Equal(3, topologyInfo.NodesById.Count);

                    var outgoingOfA = topologyInfo.NodesById[ADocumentDatabase.DbId.ToString()].Outgoing;
                    Assert.Equal(1, outgoingOfA.Count);
                    Assert.Equal(BDocumentDatabase.DbId.ToString(), outgoingOfA.First().DbId);

                    B.Dispose();
                    B = null;

                    topologyInfo = GetLiveTopology(A);

                    //C is unreachable after B is down
                    Assert.Equal(1, topologyInfo.NodesById.Count);
                    Assert.Equal(0, topologyInfo.NodesById[ADocumentDatabase.DbId.ToString()].Outgoing.Count);

                    var offlineOfA = topologyInfo.NodesById[ADocumentDatabase.DbId.ToString()].Offline;
                    var destinations = ADocumentDatabase.ReplicationLoader.Destinations.ToList();
                    Assert.Equal(1, destinations.Count); //sanity check, should always be true
                    var urlOfB = destinations[0].Url;
                    var nameOfB = destinations[0].Database;

                    Assert.Equal(1, offlineOfA.Count);
                    Assert.Equal(urlOfB, offlineOfA.First().Url);
                    Assert.Equal(nameOfB, offlineOfA.First().Database);
                }

            }
            finally
            {
                B?.Dispose();
            }
        }

        [Fact]
        public async Task Master_with_offline_slaves_should_be_properly_detected_in_live_topology()
        {
            using (var master = GetDocumentStore())
            {
                var destinations = new[]
                {
                    new ReplicationNode
                    {
                        Database = "FooBar",
                        Url = "http://foo.bar/:1234"
                    },
                    new ReplicationNode
                    {
                        Database = "FooBar2",
                        Url = "http://foo.bar/:4567"
                    }
                };

                await SetupReplicationWithCustomDestinations(master, destinations);

                var topologyInfo = GetLiveTopology(master);

                Assert.NotNull(topologyInfo); //sanity check
                Assert.Equal(1, topologyInfo.NodesById.Count);

                var inactiveNodeStatuses =
                    topologyInfo.NodesById.First().Value.Offline.Concat(
                        topologyInfo.FailedToReach
                        )
                        .GroupBy(x=>new {x.Url, x.Database})
                        .Select(g=>g.First())
                        .ToList();

                Assert.Equal(2, inactiveNodeStatuses.Count);

                var offlineNodes = inactiveNodeStatuses;
                Assert.True(offlineNodes.Any(x => x.Url == "http://foo.bar/:1234" && x.Database == "FooBar"));
                Assert.True(offlineNodes.Any(x => x.Url == "http://foo.bar/:4567" && x.Database == "FooBar2"));
            }
        }

        [Fact]
        public async Task Master_two_slaves_live_topology_should_be_correctly_detected()
        {
            using (var master = GetDocumentStore())
            using (var slave1 = GetDocumentStore())
            using (var slave2 = GetDocumentStore())
            {
                var masterDocumentDatabase = await GetDocumentDatabaseInstanceFor(master);
                var slave1DocumentDatabase = await GetDocumentDatabaseInstanceFor(slave1);
                var slave2DocumentDatabase = await GetDocumentDatabaseInstanceFor(slave2);

                await SetupReplicationAsync(master, slave1, slave2);
                EnsureReplicating(master, slave1);
                EnsureReplicating(master, slave2);

                var topologyInfo = GetLiveTopology(master);

                Assert.NotNull(topologyInfo); //sanity check
                Assert.Equal(3, topologyInfo.NodesById.Count);
                var masterOutgoing = topologyInfo.NodesById[masterDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(2, masterOutgoing.Count);
                Assert.True(masterOutgoing.Select(x => x.DbId).Any(x => x == slave1DocumentDatabase.DbId.ToString()));
                Assert.True(masterOutgoing.Select(x => x.DbId).Any(x => x == slave2DocumentDatabase.DbId.ToString()));
            }
        }

        [Fact]
        public async Task Master_master_live_topology_should_be_correctly_detected()
        {
            using (var nodeA = GetDocumentStore())
            using (var nodeB = GetDocumentStore())
            {
                //add some documents so incoming replication handlers will be initialized
                using (var session = nodeA.OpenSession())
                {
                    session.Store(new { Foo = "Bar" }, "users/1");
                    session.SaveChanges();
                }
                using (var session = nodeB.OpenSession())
                {
                    session.Store(new { Foo = "Bar2" }, "users/2");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(nodeA, nodeB);
                await SetupReplicationAsync(nodeB, nodeA);

                EnsureReplicating(nodeA, nodeB);
                EnsureReplicating(nodeB, nodeA);

                WaitForDocument(nodeA, "users/2");
                WaitForDocument(nodeB, "users/1");

                var nodeADocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeA);
                var nodeBDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeB);

                var topologyInfo = GetLiveTopology(nodeA);
                Assert.Equal(2, topologyInfo.NodesById.Count);

                var nodeAOutgoing = topologyInfo.NodesById[nodeADocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, nodeAOutgoing.Count);
                Assert.Equal(nodeBDocumentDatabase.DbId.ToString(), nodeAOutgoing.First().DbId);
                var nodeAIncoming = topologyInfo.NodesById[nodeADocumentDatabase.DbId.ToString()].Incoming;
                Assert.Equal(1, nodeAIncoming.Count);
                Assert.Equal(nodeBDocumentDatabase.DbId.ToString(), nodeAIncoming.First().DbId);

                var nodeBOutgoing = topologyInfo.NodesById[nodeBDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, nodeBOutgoing.Count);
                Assert.Equal(nodeADocumentDatabase.DbId.ToString(), nodeBOutgoing.First().DbId);
                var nodeBIncoming = topologyInfo.NodesById[nodeBDocumentDatabase.DbId.ToString()].Incoming;
                Assert.Equal(1, nodeBIncoming.Count);
                Assert.Equal(nodeADocumentDatabase.DbId.ToString(), nodeBIncoming.First().DbId);

            }
        }

        [Fact]
        public async Task Master_master_master_live_topology_should_be_correctly_detected()
        {
            using (var nodeA = GetDocumentStore())
            using (var nodeB = GetDocumentStore())
            using (var nodeC = GetDocumentStore())
            {
                await SetupReplicationAsync(nodeA, nodeB);
                await SetupReplicationAsync(nodeB, nodeC);
                await SetupReplicationAsync(nodeC, nodeA);

                EnsureReplicating(nodeA, nodeB);
                EnsureReplicating(nodeB, nodeC);
                EnsureReplicating(nodeC, nodeA);

                var nodeADocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeA);
                var nodeBDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeB);
                var nodeCDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeC);

                var topologyInfo = GetLiveTopology(nodeA);
                Assert.Equal(3, topologyInfo.NodesById.Count);

                var nodeAOutgoing = topologyInfo.NodesById[nodeADocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, nodeAOutgoing.Count);
                Assert.Equal(nodeBDocumentDatabase.DbId.ToString(), nodeAOutgoing.First().DbId);

                var nodeBOutgoing = topologyInfo.NodesById[nodeBDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, nodeBOutgoing.Count);
                Assert.Equal(nodeCDocumentDatabase.DbId.ToString(), nodeBOutgoing.First().DbId);

                var nodeCOutgoing = topologyInfo.NodesById[nodeCDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, nodeCOutgoing.Count);
                Assert.Equal(nodeADocumentDatabase.DbId.ToString(), nodeCOutgoing.First().DbId);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task Cluster_with_master_master_master_and_some_leaves_live_topology_should_be_correctly_detected(bool useSsl)
        {
            if (useSsl)
            {
                var tempPath = GenerateAndSaveSelfSignedCertificate();
                DoNotReuseServer(new ConcurrentDictionary<string, string>
                {
                    ["Raven/Certificate/Path"] = tempPath,
                    ["Raven/ServerUrl"] = "https://127.0.0.1:0"
                });
            }
            using (var nodeA = GetDocumentStore())
            using (var nodeB = GetDocumentStore())
            using (var nodeC = GetDocumentStore())
            using (var nodeALeaf = GetDocumentStore())
            using (var nodeCLeaf = GetDocumentStore())
            {
                await SetupReplicationAsync(nodeA, nodeB, nodeALeaf);
                await SetupReplicationAsync(nodeB, nodeC);
                await SetupReplicationAsync(nodeC, nodeA, nodeCLeaf);

                EnsureReplicating(nodeA, nodeB);
                EnsureReplicating(nodeA, nodeALeaf);
                EnsureReplicating(nodeB, nodeC);
                EnsureReplicating(nodeC, nodeA);
                EnsureReplicating(nodeC, nodeCLeaf);

                var nodeADocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeA);
                var nodeALeafDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeALeaf);
                var nodeBDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeB);
                var nodeCDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeC);
                var nodeCLeafDocumentDatabase = await GetDocumentDatabaseInstanceFor(nodeCLeaf);

                var topologyInfo = GetLiveTopology(nodeA);
                Assert.Equal(5, topologyInfo.NodesById.Count);

                var nodeAOutgoing = topologyInfo.NodesById[nodeADocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(2, nodeAOutgoing.Count);
                Assert.True(nodeAOutgoing.Select(x => x.DbId).Any(x => x == nodeBDocumentDatabase.DbId.ToString()));
                Assert.True(nodeAOutgoing.Select(x => x.DbId).Any(x => x == nodeALeafDocumentDatabase.DbId.ToString()));

                var nodeBOutgoing = topologyInfo.NodesById[nodeBDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, nodeBOutgoing.Count);
                Assert.Equal(nodeCDocumentDatabase.DbId.ToString(), nodeBOutgoing.First().DbId);

                var nodeCOutgoing = topologyInfo.NodesById[nodeCDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(2, nodeCOutgoing.Count);
                Assert.True(nodeCOutgoing.Select(x => x.DbId).Any(x => x == nodeADocumentDatabase.DbId.ToString()));
                Assert.True(nodeCOutgoing.Select(x => x.DbId).Any(x => x == nodeCLeafDocumentDatabase.DbId.ToString()));
            }
        }

        [Fact]
        public async Task Master_slave_slaveOfslave_live_topology_should_be_correctly_detected()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            using (var slaveOfSlave = GetDocumentStore())
            {
                await SetupReplicationAsync(master, slave);
                await SetupReplicationAsync(slave, slaveOfSlave);

                EnsureReplicating(master, slave);

                EnsureReplicating(slave, slaveOfSlave);

                var masterDocumentDatabase = await GetDocumentDatabaseInstanceFor(master);
                masterDocumentDatabase.Configuration.Replication.ReplicationTopologyDiscoveryTimeout = new TimeSetting(24, TimeUnit.Hours);
                var slaveDocumentDatabase = await GetDocumentDatabaseInstanceFor(slave);
                slaveDocumentDatabase.Configuration.Replication.ReplicationTopologyDiscoveryTimeout = new TimeSetting(24, TimeUnit.Hours);
                var slaveOfSlaveDocumentDatabase = await GetDocumentDatabaseInstanceFor(slaveOfSlave);
                slaveOfSlaveDocumentDatabase.Configuration.Replication.ReplicationTopologyDiscoveryTimeout = new TimeSetting(24, TimeUnit.Hours);

                var topologyInfo = GetLiveTopology(master);
                Assert.Equal(3, topologyInfo.NodesById.Count);
                var masterNodeOutgoing = topologyInfo.NodesById[masterDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, masterNodeOutgoing.Count);

                Assert.Equal(slaveDocumentDatabase.DbId.ToString(), masterNodeOutgoing.First().DbId);

                var slaveNodeOutgoing = topologyInfo.NodesById[slaveDocumentDatabase.DbId.ToString()].Outgoing;
                Assert.Equal(1, slaveNodeOutgoing.Count);

                Assert.Equal(slaveOfSlaveDocumentDatabase.DbId.ToString(), slaveNodeOutgoing.First().DbId);
            }
        }
    }
}
