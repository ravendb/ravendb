using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests.DatabaseCluster
{
    public class RemoveNodeFromCluster : ClusterTestBase
    {
        public RemoveNodeFromCluster(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task RemovedNodeChangeReplicationFactor()
        {
            var dbName = GetDatabaseName();
            await RemoveNodeWithDatabase(dbName, 5, 5);
        }

        [Fact]
        public async Task ReconnectRemovedNodeWithDatabases()
        {
            var dbName = GetDatabaseName();
            var removed = await RemoveNodeWithDatabase(dbName, 5, 5);

            RavenServer leaderNode;
            using (var store = new DocumentStore
            {
                Urls = new[] { removed.WebUrl },
                Database = dbName,
            }.Initialize())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(5, record.Topology.Count);
                Assert.Equal(5, record.Topology.ReplicationFactor);

                //reconnect the removed node to the original cluster
                leaderNode = await ActionWithLeader(leader => leader.ServerStore.AddNodeToClusterAsync(removed.WebUrl, removed.ServerStore.NodeTag));
                Assert.True(await removed.ServerStore.WaitForState(RachisState.Follower, CancellationToken.None).WaitWithoutExceptionAsync(TimeSpan.FromSeconds(30)),
                    "Removed node wasn't reconnected with the cluster.");
                await removed.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, leaderNode.ServerStore.LastRaftCommitIndex);
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));

                Assert.Equal(4, record.Topology.Count);
                Assert.Equal(4, record.Topology.ReplicationFactor);
            }

            using (var store = new DocumentStore
            {
                Urls = new[] { leaderNode.WebUrl },
                Database = dbName,
            }.Initialize())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(4, record.Topology.Count);
                Assert.Equal(4, record.Topology.ReplicationFactor);
            }
        }

        [Fact]
        public async Task BootstrapRemovedNode()
        {
            var dbName = GetDatabaseName();
            var removed = await RemoveNodeWithDatabase(dbName, 5, 5);

            using (var store = new DocumentStore
            {
                Urls = new[] { removed.WebUrl },
                Database = dbName,
            }.Initialize())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(5, record.Topology.Count);
                Assert.Equal(5, record.Topology.ReplicationFactor);

                //bootstrap the removed node to a single-node cluster
                await removed.ServerStore.EnsureNotPassiveAsync();
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(1, record.Topology.Count);
                Assert.Equal(1, record.Topology.ReplicationFactor);

                Assert.True(WaitForDocument(store, "foo/bar"));
            }
        }

        [Theory]
        [InlineData("A")]
        [InlineData("B")]
        [InlineData("ONE")]
        public async Task HardResetToNewClusterTest(string tag)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var dbName = GetDatabaseName();
            var dbName2 = GetDatabaseName();

            var cluster = await CreateRaftCluster(2);
            await CreateDatabaseInCluster(dbName, 2, cluster.Leader.WebUrl);
            await CreateDatabaseInCluster(dbName2, 2, cluster.Leader.WebUrl);
            var node = cluster.Nodes.First(x => x != cluster.Leader);

            using (var store = new DocumentStore
            {
                Urls = new[] { cluster.Leader.WebUrl },
                Database = dbName,
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                Urls = new[] { node.WebUrl },
                Database = dbName,
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                var result = WaitForDocument(store2, "foo/bar");
                Assert.True(result);

                await cluster.Leader.ServerStore.Engine.HardResetToNewClusterAsync(tag);
                await AssertWaitForTrueAsync(() => Task.FromResult(node.ServerStore.Engine.CurrentCommittedState.State == RachisState.Passive));

                var outgoingConnections = await WaitForValueAsync(async () =>
                {
                    var dbInstance = await cluster.Leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName);
                    return dbInstance.ReplicationLoader.OutgoingHandlers.Count();
                }, 0);

                Assert.Equal(0, outgoingConnections);

                foreach (var n in cluster.Nodes)
                    await n.ServerStore.EnsureNotPassiveAsync();

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "foo/bar/2");
                    await session.SaveChangesAsync();
                }

                await AssertWaitForValueAsync(() => GetTopologyNodesCount(store), 1);
                await AssertWaitForValueAsync(() => GetTopologyNodesCount(store2), 1);

                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("foo/bar");
                    var user2 = await session.LoadAsync<User>("foo/bar/2");

                    Assert.NotNull(user);
                    Assert.Null(user2);
                }
            }
        }

        private static async Task<int> GetTopologyNodesCount(IDocumentStore store)
        {
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            return record == null ? -1 : record.Topology.Members.Count + record.Topology.Promotables.Count + record.Topology.Rehabs.Count;
        }

        [Theory]
        [InlineData("A")]
        [InlineData("B")]
        [InlineData("ONE")]
        public async Task HardResetToPassive(string tag)
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var dbName = GetDatabaseName();
            var dbName2 = GetDatabaseName();

            var cluster = await CreateRaftCluster(2);
            await CreateDatabaseInCluster(dbName, 2, cluster.Leader.WebUrl);
            await CreateDatabaseInCluster(dbName2, 2, cluster.Leader.WebUrl);
            var node = cluster.Nodes.First(x => x != cluster.Leader);

            using (var store = new DocumentStore
            {
                Urls = new[] { cluster.Leader.WebUrl },
                Database = dbName,
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                Urls = new[] { node.WebUrl },
                Database = dbName,
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                var result = WaitForDocument(store2, "foo/bar");
                Assert.True(result);

                await cluster.Leader.ServerStore.Engine.HardResetToPassiveAsync(Guid.NewGuid().ToString());
                await cluster.Leader.ServerStore.EnsureNotPassiveAsync(nodeTag: tag);

                var outgoingConnections = await WaitForValueAsync(async () =>
                {
                    var dbInstance = await cluster.Leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName);
                    return dbInstance.ReplicationLoader.OutgoingHandlers.Count();
                }, 0);

                Assert.Equal(0, outgoingConnections);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Karmel" }, "foo/bar/2");
                    await session.SaveChangesAsync();
                }
                using (var session = store2.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("foo/bar");
                    var user2 = await session.LoadAsync<User>("foo/bar/2");

                    Assert.NotNull(user);
                    Assert.Null(user2);
                }
            }
        }

        [Fact]
        public async Task RetainDatabasesAfterRemovingLastNodeFromCluster()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;

            var dbName = GetDatabaseName();

            var cluster = await CreateRaftCluster(2, shouldRunInMemory: false);

            var leaderNode = cluster.Leader.ServerStore.NodeTag;
            var memberNode = cluster.Nodes.First(x => x != cluster.Leader).ServerStore.NodeTag;

            await CreateDatabaseInCluster(new DatabaseRecord(dbName)
            {
                Topology = new DatabaseTopology
                {
                    Members = new List<string>
                    {
                        leaderNode
                    },
                    ReplicationFactor = 1
                }
            }, 1, cluster.Leader.WebUrl);

            using (var store = new DocumentStore
            {
                Urls = new[] { cluster.Leader.WebUrl },
                Database = dbName,
            }.Initialize())
            {
                await ActionWithLeader((l) => l.ServerStore.RemoveFromClusterAsync(memberNode));
                var result = await DisposeServerAndWaitForFinishOfDisposalAsync(cluster.Leader);
                cluster.Leader = GetNewServer(new ServerCreationOptions
                {
                    DeletePrevious = false,
                    RunInMemory = false,
                    DataDirectory = result.DataDirectory,
                    CustomSettings = new Dictionary<string, string>
                    {
                        [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url
                    }
                });

                await cluster.Leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None);
                Assert.NotNull(await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName)));
            }
        }

        [Fact]
        public async Task DontKickFromClusterOnElectionTimeoutMismatch()
        {
            var cluster = await CreateRaftCluster(2, shouldRunInMemory: false);
            var result = await DisposeServerAndWaitForFinishOfDisposalAsync(cluster.Nodes[0]);
            await cluster.Nodes[1].ServerStore.WaitForState(RachisState.Candidate, CancellationToken.None);
            cluster.Nodes[0] = GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                RunInMemory = false,
                DataDirectory = result.DataDirectory,
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url,
                    [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = 600.ToString(),
                }
            });

            using (var cts = new CancellationTokenSource(10_000))
            {
                var t1 = cluster.Nodes[0].ServerStore.WaitForState(RachisState.Leader, cts.Token);
                var t2 = cluster.Nodes[1].ServerStore.WaitForState(RachisState.Leader, cts.Token);

                var task = await Task.WhenAny(t1, t2);
                if (task == t1)
                {
                    Assert.NotEqual(RachisState.Passive, cluster.Nodes[1].ServerStore.Engine.CurrentCommittedState.State);
                }
                else
                {
                    Assert.NotEqual(RachisState.Passive, cluster.Nodes[0].ServerStore.Engine.CurrentCommittedState.State);
                }
            }

            result = await DisposeServerAndWaitForFinishOfDisposalAsync(cluster.Nodes[1]);
            cluster.Nodes[1] = GetNewServer(new ServerCreationOptions
            {
                DeletePrevious = false,
                RunInMemory = false,
                DataDirectory = result.DataDirectory,
                CustomSettings = new Dictionary<string, string>
                {
                    [RavenConfiguration.GetKey(x => x.Core.ServerUrls)] = result.Url,
                    [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = 600.ToString(),
                }
            });

            using (var cts = new CancellationTokenSource(10_000))
            {
                var t1 = cluster.Nodes[0].ServerStore.WaitForState(RachisState.Leader, cts.Token);
                var t2 = cluster.Nodes[1].ServerStore.WaitForState(RachisState.Leader, cts.Token);

                var task = await Task.WhenAny(t1, t2);
                if (task == t1)
                {
                    Assert.Equal(RachisState.Follower, cluster.Nodes[1].ServerStore.Engine.CurrentCommittedState.State);
                }
                else
                {
                    Assert.Equal(RachisState.Follower, cluster.Nodes[0].ServerStore.Engine.CurrentCommittedState.State);
                }
            }
        }

        [Fact]
        public async Task ReconnectRemovedNodeWithOneDatabase()
        {
            // BAD IDEA - we lose the database!
            var dbName = GetDatabaseName();
            var removed = await RemoveNodeWithDatabase(dbName, 5, 1);

            using (var store = new DocumentStore
            {
                Urls = new[] { removed.WebUrl },
                Database = dbName,
            }.Initialize())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.True(record != null, $"record is null: {Cluster.GetRaftHistory(removed)}");
                Assert.True(record.Topology != null, $"topology is null: {Cluster.GetRaftHistory(removed)}");

                Assert.Equal(1, record.Topology.Count);
                Assert.Equal(1, record.Topology.ReplicationFactor);

                //reconnect the removed node to the original cluster
                var leaderNode = await ActionWithLeader(leader => leader.ServerStore.AddNodeToClusterAsync(removed.WebUrl, removed.ServerStore.NodeTag));
                Assert.True(await removed.ServerStore.WaitForState(RachisState.Follower, CancellationToken.None).WaitWithoutExceptionAsync(TimeSpan.FromSeconds(30)),
                    "Removed node wasn't reconnected with the cluster.");
                await removed.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, leaderNode.ServerStore.LastRaftCommitIndex);
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Null(record);
            }
        }

        [Fact]
        public async Task BootstrapRemovedNodeWithOneDatabase()
        {
            var dbName = GetDatabaseName();
            var removed = await RemoveNodeWithDatabase(dbName, 5, 1);

            using (var store = new DocumentStore
            {
                Urls = new[] { removed.WebUrl },
                Database = dbName,
            }.Initialize())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(1, record.Topology.Count);
                Assert.Equal(1, record.Topology.ReplicationFactor);

                //bootstrap the removed node to a single-node cluster
                await removed.ServerStore.EnsureNotPassiveAsync();
                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));
                Assert.Equal(1, record.Topology.Count);
                Assert.Equal(1, record.Topology.ReplicationFactor);

                Assert.True(WaitForDocument(store, "foo/bar"));
            }
        }

        [InlineData(3)]
        [InlineData(5)]
        [Theory]
        public async Task RemovedLeaderCauseReelection(int numberOfNodes)
        {
            var (_, leader) = await CreateRaftCluster(numberOfNodes);
            using (var cts = new CancellationTokenSource())
            {
                try
                {
                    var followerTasks = Servers.Where(s => s != leader).Select(s => s.ServerStore.WaitForState(RachisState.Leader, cts.Token));
                    await ActionWithLeader(l => l.ServerStore.RemoveFromClusterAsync(leader.ServerStore.NodeTag));
                    Assert.True(await Task.WhenAny(followerTasks).WaitWithoutExceptionAsync(TimeSpan.FromSeconds(30)));
                }
                finally
                {
                    cts.Cancel();
                }
            }
        }

        private async Task<RavenServer> RemoveNodeWithDatabase(string dbName, int nodesAmount, int replicationFactor)
        {
            var (_, firstLeader) = await CreateRaftCluster(nodesAmount, leaderIndex: 0);
            var (_, servers) = await CreateDatabaseInCluster(dbName, replicationFactor, firstLeader.WebUrl);
            var removed = servers.Last();
            using (var store = new DocumentStore
            {
                Urls = new[] { firstLeader.WebUrl },
                Database = dbName
            }.Initialize())
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(TimeSpan.FromSeconds(30), replicas: replicationFactor - 1);
                    await session.StoreAsync(new User { Name = "Karmel" }, "foo/bar");
                    await session.SaveChangesAsync();
                }

                await ActionWithLeader(l => l.ServerStore.RemoveFromClusterAsync(removed.ServerStore.NodeTag));
                Assert.True(await removed.ServerStore.WaitForState(RachisState.Passive, CancellationToken.None).WaitWithoutExceptionAsync(TimeSpan.FromSeconds(30)),
                    $"Removed node wasn't move to passive state ({removed.ServerStore.Engine.CurrentCommittedState.State})");

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(dbName));

                if (removed.WebUrl == firstLeader.WebUrl)
                {
                    Assert.Equal(replicationFactor, record.Topology.Count);
                    Assert.Equal(replicationFactor, record.Topology.ReplicationFactor);
                    return removed;
                }

                if (replicationFactor == 1)
                {
                    // if we remove the only node that have the database, it should delete the record in the cluster.
                    Assert.Null(record);
                    return removed;
                }

                Assert.Equal(replicationFactor - 1, record.Topology.Count);
                Assert.Equal(replicationFactor - 1, record.Topology.ReplicationFactor);
            }
            return removed;
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task NodeShouldBeRemovedFromPriorityOrder()
        {
            const int clusterSize = 3;
            var cluster = await CreateRaftCluster(clusterSize, leaderIndex: 0, watcherCluster: true);
            var order = new List<string> { "A", "B", "C" };

            using (var store = GetDocumentStore(new Options
                   {
                       Server = cluster.Leader,
                       ReplicationFactor = clusterSize,
                       ModifyDatabaseRecord = x => x.Topology = new DatabaseTopology
                       {
                           Members = order,
                           ReplicationFactor = 3,
                           PriorityOrder = order
                       }
                   }))
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.True(order.All(x => record.Topology.PriorityOrder.Contains(x)));

                var toRemove = cluster.Nodes.First(x => x.ServerStore.NodeTag != cluster.Leader.ServerStore.NodeTag);
                var removed = await DisposeServerAndWaitForFinishOfDisposalAsync(toRemove);
                await ActionWithLeader(l => l.ServerStore.RemoveFromClusterAsync(removed.NodeTag));

                await WaitAndAssertForValueAsync(async () =>
                {
                    record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Topology.PriorityOrder.Contains(removed.NodeTag);
                }, false);
            }
        }
    }
}
