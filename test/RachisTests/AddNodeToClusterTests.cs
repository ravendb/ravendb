using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests
{
    public class AddNodeToClusterTests : ReplicationTestBase
    {
        [NightlyBuildFact]
        public async Task FailOnAddingNonPassiveNode()
        {
            var raft1 = await CreateRaftClusterAndGetLeader(1);
            var raft2 = await CreateRaftClusterAndGetLeader(1);

            var url = raft2.WebUrl;
            await raft1.ServerStore.AddNodeToClusterAsync(url);
            Assert.True(await WaitForValueAsync(() => raft1.ServerStore.GetClusterErrors().Count > 0, true));
        }


        [NightlyBuildFact]
        public async Task RemoveNodeWithDb()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var fromSeconds = Debugger.IsAttached ? TimeSpan.FromSeconds(15) : TimeSpan.FromSeconds(5);

            var leader = await CreateRaftClusterAndGetLeader(5);
            Assert.True(leader.ServerStore.LicenseManager.HasHighlyAvailableTasks());

            var db = await CreateDatabaseInCluster("MainDB", 5, leader.WebUrl);
            var watcherDb = await CreateDatabaseInCluster("WatcherDB", 1, leader.WebUrl);
            var serverNodes = db.Servers.Select(s => new ServerNode
            {
                ClusterTag = s.ServerStore.NodeTag,
                Database = "MainDB",
                Url = s.WebUrl
            }).ToList();

            var conventions = new DocumentConventions
            {
                DisableTopologyUpdates = true
            };

            using (var watcherStore = new DocumentStore
            {
                Database = "WatcherDB",
                Urls = new[] { watcherDb.Item2.Single().WebUrl },
                Conventions = conventions
            }.Initialize())
            using (var leaderStore = new DocumentStore
            {
                Database = "MainDB",
                Urls = new[] { leader.WebUrl },
                Conventions = conventions
            }.Initialize())
            {
                var watcher = new ExternalReplication("WatcherDB", "Connection")
                {
                    MentorNode = Servers.First(s => s.ServerStore.NodeTag != watcherDb.Servers[0].ServerStore.NodeTag).ServerStore.NodeTag
                };

                Assert.True(watcher.MentorNode != watcherDb.Servers[0].ServerStore.NodeTag);

                var watcherRes = await AddWatcherToReplicationTopology((DocumentStore)leaderStore, watcher);
                var tasks = new List<Task>();
                foreach (var ravenServer in Servers)
                {
                    tasks.Add(ravenServer.ServerStore.Cluster.WaitForIndexNotification(watcherRes.RaftCommandIndex));
                }

                Assert.True(await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(5)));

                var responsibleServer = Servers.Single(s => s.ServerStore.NodeTag == watcherRes.ResponsibleNode);
                using (var responsibleStore = new DocumentStore
                {
                    Database = "MainDB",
                    Urls = new[] { responsibleServer.WebUrl },
                    Conventions = conventions
                }.Initialize())
                {
                    // check that replication works.
                    using (var session = leaderStore.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = "Karmel"
                        }, "users/1");
                        session.SaveChanges();
                    }

                    Assert.True(await WaitForDocumentInClusterAsync<User>(serverNodes, "users/1", u => u.Name == "Karmel", fromSeconds));
                    Assert.True(WaitForDocument<User>(watcherStore, "users/1", u => u.Name == "Karmel", 30_000));

                    // remove the node from the cluster that is responsible for the external replication
                    Assert.True(await leader.ServerStore.RemoveFromClusterAsync(watcherRes.ResponsibleNode).WaitAsync(fromSeconds));
                    Assert.True(await responsibleServer.ServerStore.WaitForState(RachisState.Passive).WaitAsync(fromSeconds));

                    var dbInstance = await responsibleServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore("MainDB");
                    await WaitForValueAsync(() => dbInstance.ReplicationLoader.OutgoingConnections.Count(), 0);

                    // replication from the removed node should be suspended
                    using (var session = responsibleStore.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User
                        {
                            Name = "Karmel2"
                        }, "users/2");
                        await session.SaveChangesAsync();
                    }
                }
                
                var nodeInCluster = serverNodes.First(s => s.ClusterTag != responsibleServer.ServerStore.NodeTag);
                using (var nodeInClusterStore = new DocumentStore
                {
                    Database = "MainDB",
                    Urls = new[] { nodeInCluster.Url },
                    Conventions = conventions
                }.Initialize())
                {
                    Assert.False(WaitForDocument<User>(nodeInClusterStore, "users/2", u => u.Name == "Karmel2"));
                    Assert.False(WaitForDocument<User>(watcherStore, "users/2", u => u.Name == "Karmel2"));

                    // the task should be reassinged within to another node
                    using (var session = nodeInClusterStore.OpenSession())
                    {
                        session.Store(new User
                        {
                            Name = "Karmel2"
                        }, "users/3");
                        session.SaveChanges();
                    }
                }

                Assert.True(WaitForDocument<User>(watcherStore, "users/3", u => u.Name == "Karmel2", 30_000));

                // rejoin the node
                var newLeader = Servers.Single(s => s.ServerStore.IsLeader());
                Assert.True(await newLeader.ServerStore.AddNodeToClusterAsync(responsibleServer.WebUrl, watcherRes.ResponsibleNode).WaitAsync(fromSeconds));
                Assert.True(await responsibleServer.ServerStore.WaitForState(RachisState.Follower).WaitAsync(fromSeconds));

                using (var session = leaderStore.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Karmel4"
                    }, "users/4");
                    await session.SaveChangesAsync();
                }
                
                foreach (var node in serverNodes)
                {
                    // after we removed watcherRes.ResponsibleNode from the cluster, it is no longer 
                    // a part of the Database-Group (even after re-adding it to the cluster)
                    if (node.ClusterTag == watcherRes.ResponsibleNode)
                        continue;

                    using (var store = new DocumentStore
                    {
                        Database = "MainDB",
                        Urls = new[] { node.Url },
                        Conventions = conventions
                    }.Initialize())
                    {
                        Assert.True(WaitForDocument<User>(store, "users/4", u => u.Name == "Karmel4", 30_000));
                    }
                }
                
                Assert.True(WaitForDocument<User>(watcherStore, "users/4", u => u.Name == "Karmel4", 30_000));
            }
        }
    }
}
