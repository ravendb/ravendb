using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Replication
{
    public class ReplicationWriteAssurance : ClusterTestBase 
    {
        public ReplicationWriteAssurance(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Replication)]
        public async Task ServerSideWriteAssurance()
        {
            var (_, leader) = await CreateRaftCluster(3);
            Cluster.SuspendObserver(leader);
            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3,
                ModifyDatabaseRecord = record =>
                {
                    record.Topology = new Raven.Client.ServerWide.DatabaseTopology
                    {
                        DynamicNodesDistribution = false,
                        Members = new System.Collections.Generic.List<string>
                        {
                            "A","B","C"
                        },
                        ReplicationFactor = 3
                    };
                }
            }))
            {
                using (var s1 = store.OpenSession())
                {
                    s1.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2, timeout: TimeSpan.FromSeconds(30));

                    s1.Store(new
                    {
                        Name = "Idan"
                    }, "users/1");

                    s1.SaveChanges();
                }

                foreach (var server in Servers)
                {
                    var db = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                    using(db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    {
                        context.OpenReadTransaction();
                        Assert.NotNull(db.DocumentsStorage.Get(context, "users/1"));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Cluster)]
        public async Task WaitForReplicationWithMajorityShouldTimeoutWhenMajorityOfDatabaseGroupIsDown()
        {
            var (nodes, leader) = await CreateRaftCluster(3);

            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 3,
                DeleteDatabaseOnDispose = false // after we take down 2 of 3 nodes there is no cluster quorum to delete the database
            }))
            {
                // make sure the replication connections to both siblings are up and the data is everywhere
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), replicas: 2);
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                var database = await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                Assert.Equal(2, database.ReplicationLoader.NumberOfSiblingsInInternalReplication);

                // take down both siblings, leaving only the node we are writing to
                foreach (var node in nodes.Where(n => n != leader))
                {
                    await DisposeServerAndWaitForFinishOfDisposalAsync(node);
                }


                // the outgoing connections to the dead nodes are dropped
                Assert.Equal(0, WaitForValue(() => database.ReplicationLoader.OutgoingConnections.Count(), 0, timeout: 60_000));

                // the dead nodes are NOT removed from the destinations: moving them to rehab requires
                // a committed database record change, and with 2 of 3 cluster nodes down there is no quorum
                Assert.Equal(2, database.ReplicationLoader.Destinations.Count);

                // they are still part of the database group, so they are still counted as siblings
                Assert.Equal(2, database.ReplicationLoader.NumberOfSiblingsInInternalReplication);

                // only 1 of the 3 copies can be written, so a majority write-assurance must fail
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), majority: true);
                    session.Store(new User(), "users/2");
                    Assert.Throws<RavenTimeoutException>(() => session.SaveChanges());
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Cluster)]
        public async Task WaitForReplicationWithMajorityShouldWaitForMajorityOfTheDatabaseGroup()
        {
            var customSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "5"
            };

            var (nodes, leader) = await CreateRaftCluster(5, customSettings: customSettings);

            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 5
            }))
            {
                // make sure the replication connections to all 4 siblings are up
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(60), replicas: 4);
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                // take down 2 of the 5 nodes, the cluster keeps its quorum (3 of 5)
                // so the observer will move both to rehab and commit the topology change
                var killed = nodes.Where(n => n != leader).Take(2).ToList();
                foreach (var node in killed)
                {
                    await DisposeServerAndWaitForFinishOfDisposalAsync(node);
                }

                var rehabs = await WaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Topology.Rehabs.Count;
                }, 2, timeout: 60_000);
                Assert.Equal(2, rehabs);

                // a majority of a 5-node database group is 3 copies, so every node must wait for 2 replicas.
                // the rehab nodes are still part of the database group and hold the data, so they are counted as siblings
                // regardless of which node mentors them
                foreach (var node in nodes.Where(n => killed.Contains(n) == false))
                {
                    var database = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                    // the rehab nodes are still part of the database group, so every node has 4 siblings
                    Assert.Equal(4, database.ReplicationLoader.NumberOfSiblingsInInternalReplication);

                    var minReplicas = WaitForValue(() => database.ReplicationLoader.GetMinNumberOfReplicas(), 2, timeout: 30_000);
                    Assert.Equal(2, minReplicas);
                }

                // 3 nodes are still up, so writing a majority (3 copies) is achievable and must succeed
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(30), majority: true);
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }
            }
        }

        [RavenFact(RavenTestCategory.Replication | RavenTestCategory.Cluster)]
        public async Task PromotableNodeShouldNotBeCountedAsSiblingInWaitForReplicationWithMajority()
        {
            var (nodes, leader) = await CreateRaftCluster(3);

            // suspend the observer (it only runs on the leader) so the added node stays in promotable state and is never promoted to member
            Cluster.SuspendObserver(leader);

            using (var store = GetDocumentStore(new Options
            {
                Server = leader,
                ReplicationFactor = 1
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }

                // find the single member node that holds the database
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record.Topology.Members.Count);
                var memberServer = nodes.Single(n => n.ServerStore.NodeTag == record.Topology.Members[0]);

                // add a second node, it enters the topology as a promotable and stays there (the observer is suspended)
                var addResult = await store.Maintenance.Server.SendAsync(new AddDatabaseNodeOperation(store.Database));
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(addResult.RaftCommandIndex, TimeSpan.FromSeconds(15));

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(1, record.Topology.Members.Count);
                Assert.Equal(1, record.Topology.Promotables.Count);

                var database = await memberServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                // the only other node is a promotable, it is a new node that doesn't hold the data yet so it isn't a sibling
                var siblings = WaitForValue(() => database.ReplicationLoader.NumberOfSiblingsInInternalReplication, 0, timeout: 15_000);
                Assert.Equal(0, siblings);
                Assert.Equal(0, database.ReplicationLoader.GetMinNumberOfReplicas());

                // there are no siblings that hold the data, so a majority write-assurance requires 0 replicas and completes immediately
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), majority: true);
                    session.Store(new User(), "users/2");
                    session.SaveChanges();
                }
            }
        }
    }
}
