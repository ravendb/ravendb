using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Documents.Indexing;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests.DatabaseCluster
{
    public class ClusterDatabaseMaintenance : ReplicationTestsBase
    {
        [Fact]
        public async Task DemoteOnServerDown()
        {
            var clusterSize = 3;
            var databaseName = "DemoteOnServerDown";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0);
            using (var store = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.Members.Count);
                Servers[1].Dispose();

                var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize - 1);
                Assert.Equal(clusterSize - 1, val);
                val = await WaitForValueAsync(async () => await GetPromotableCount(store, databaseName), 1);
                Assert.Equal(1, val);
            }
        }

        [Fact]
        public async Task PromoteOnCatchingUp()
        {
            var clusterSize = 3;
            var databaseName = "PromoteOnCatchingUp";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0);
            using (var store = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var createRes = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc));

                var member = createRes.Topology.Members.Single();

                var dbServer = Servers.Single(s => s.ServerStore.NodeTag == member);
                await dbServer.ServerStore.Cluster.WaitForIndexNotification(createRes.RaftCommandIndex);

                await dbServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);

                using (var dbStore = new DocumentStore
                {
                    Urls = dbServer.WebUrls,
                    Database = databaseName
                }.Initialize())
                {
                    using (var session = dbStore.OpenAsyncSession())
                    {
                        await session.StoreAsync(new IndexMerging.User { Name = "Karmel" }, "users/1");
                        await session.SaveChangesAsync();
                    }
                }
                var res = await store.Admin.Server.SendAsync(new AddDatabaseNodeOperation(databaseName));
                Assert.Equal(1, res.Topology.Members.Count);
                Assert.Equal(1, res.Topology.Promotables.Count);

                await WaitForRaftIndexToBeAppliedInCluster(res.RaftCommandIndex, TimeSpan.FromSeconds(5));
                await WaitForDocumentInClusterAsync<IndexMerging.User>(res.Topology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(10));

                var val = await WaitForValueAsync(async () => await GetPromotableCount(store, databaseName), 0);
                Assert.Equal(0, val);
                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2);
                Assert.Equal(2, val);
            }
        }

        [Fact]
        public async Task SuccessfulMaintenanceOnLeaderChange()
        {
            var clusterSize = 3;
            var databaseName = "SuccessfulMaintenanceOnLeaderChange";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0);
            using (var store = new DocumentStore()
            {
                Urls = leader.WebUrls,
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var res = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                await WaitForRaftIndexToBeAppliedInCluster(res.RaftCommandIndex, TimeSpan.FromSeconds(5));
                Assert.Equal(3, res.Topology.Members.Count);
            }

            leader.Dispose();

            using (var store = new DocumentStore()
            {
                Urls = Servers[1].WebUrls,
                Database = databaseName
            }.Initialize())
            {
                var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2);
                Assert.Equal(2, val);
                val = await WaitForValueAsync(async () => await GetPromotableCount(store, databaseName), 1);
                Assert.Equal(1, val);
            }
        }

        [Fact]
        public async Task PromoteDatabaseNodeBackAfterReconnection()
        {
            var clusterSize = 3;
            var databaseName = "PromoteDatabaseNodeBackAfterReconnection";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);
            using (var store = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.Members.Count);
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(10));
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new IndexMerging.User());
                    await session.SaveChangesAsync();
                }
                var urls = Servers[1].WebUrls;
                var dataDir = Servers[1].Configuration.Core.DataDirectory.FullPath.Split('/').Last();
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);

                var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize - 1);
                Assert.Equal(clusterSize - 1, val);
                val = await WaitForValueAsync(async () => await GetPromotableCount(store, databaseName), 1);
                Assert.Equal(1, val);
                Servers[1] = GetNewServer(new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrl), urls[0] } }, runInMemory: false, deletePrevious: false, partialPath: dataDir);
                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 3, 30_000);
                Assert.Equal(3, val);
                val = await WaitForValueAsync(async () => await GetPromotableCount(store, databaseName), 0, 30_000);
                Assert.Equal(0, val);
            }
        }

        [Fact]
        public async Task MoveToPassiveWhenRefusedConnectionFromAllNodes()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 3;
            var databaseName = "MoveToPassiveWhenRefusedConnectionFromAllNodes";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);

            using (var store = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.Members.Count);
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(10));
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new IndexMerging.User());
                    await session.SaveChangesAsync();
                }
                var dataDir = Servers[1].Configuration.Core.DataDirectory.FullPath.Split('/').Last();
                var urls = Servers[1].WebUrls;
                var nodeTag = Servers[1].ServerStore.NodeTag;
                // kill the process and remove the node from topology
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);
                await leader.ServerStore.RemoveFromClusterAsync(nodeTag);
                using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    var val = await WaitForValueAsync(() =>
                    {
                        using (context.OpenReadTransaction())
                        {
                            return Servers[2].ServerStore.GetClusterTopology(context).AllNodes.Count;
                        }
                    }, clusterSize - 1);
                    Assert.Equal(clusterSize - 1, val);
                    val = await WaitForValueAsync(() =>
                    {
                        using (context.OpenReadTransaction())
                        {
                            return Servers[0].ServerStore.GetClusterTopology(context).AllNodes.Count;
                        }
                    }, clusterSize - 1);
                    Assert.Equal(clusterSize - 1, val);
                }
                // bring the node back to live and ensure that he moves to passive state
                Servers[1] = GetNewServer(new Dictionary<string, string> { { RavenConfiguration.GetKey(x => x.Core.ServerUrl), urls[0] } }, runInMemory: false, deletePrevious: false, partialPath: dataDir);
                await Servers[1].ServerStore.WaitForState(RachisConsensus.State.Passive).WaitAsync(TimeSpan.FromSeconds(30));
                Assert.Equal(RachisConsensus.State.Passive, Servers[1].ServerStore.CurrentState);
                // rejoin the node to the cluster
                await leader.ServerStore.AddNodeToClusterAsync(urls[0], nodeTag);
                await Servers[1].ServerStore.WaitForState(RachisConsensus.State.Follower).WaitAsync(TimeSpan.FromSeconds(30));
                Assert.Equal(RachisConsensus.State.Follower, Servers[1].ServerStore.CurrentState);
            }
        }

        [Fact]
        public async Task RedistrebuteDatabaseIfNodeFailes()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 3;
            var dbGroupSize = 2;
            var databaseName = "RedistrebuteDatabaseIfNodeFailes";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);

            using (var store = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                doc.Topology = new DatabaseTopology();
                doc.Topology.Members.Add("A");
                doc.Topology.Members.Add("B");
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, dbGroupSize));
                Assert.Equal(dbGroupSize, databaseResult.Topology.Members.Count);
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(10));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new IndexMerging.User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<IndexMerging.User>(doc.Topology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(5)));
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);

                // the db should move from node B to node C
                var newTopology = new DatabaseTopology();
                newTopology.Members.Add("A");
                newTopology.Members.Add("C");
                Assert.True(await WaitForDocumentInClusterAsync<IndexMerging.User>(newTopology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(60)));
                var members = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2, 30_000);
                Assert.Equal(2, members);
            }
        }

        [Fact]
        public async Task RedistrebuteDatabaseOnMultiFailure()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var clusterSize = 5;
            var dbGroupSize = 3;
            var databaseName = "RedistrebuteDatabaseOnCascadeFailure";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);

            using (var store = new DocumentStore
            {
                Urls = leader.WebUrls,
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                doc.Topology = new DatabaseTopology();
                doc.Topology.Members.Add("A");
                doc.Topology.Members.Add("B");
                doc.Topology.Members.Add("C");
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, dbGroupSize));
                Assert.Equal(dbGroupSize, databaseResult.Topology.Members.Count);
                await WaitForRaftIndexToBeAppliedInCluster(databaseResult.RaftCommandIndex, TimeSpan.FromSeconds(10));
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new IndexMerging.User { Name = "Karmel" }, "users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<IndexMerging.User>(doc.Topology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(5)));
                DisposeServerAndWaitForFinishOfDisposal(Servers[1]);
                WaitForUserToContinueTheTest((DocumentStore)store);
                DisposeServerAndWaitForFinishOfDisposal(Servers[2]);

                // the db should move to D & E
                var newTopology = new DatabaseTopology();
                newTopology.Members.Add("A");
                newTopology.Members.Add("D");
                newTopology.Members.Add("E");
                Assert.True(await WaitForDocumentInClusterAsync<IndexMerging.User>(newTopology, databaseName, "users/1", u => u.Name == "Karmel", TimeSpan.FromSeconds(60)));
                var members = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 3, 30_000);
                Assert.Equal(3, members);
            }
        }

        private static async Task<int> GetPromotableCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Admin.Server.SendAsync(new GetDatabaseTopologyOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Promotables.Count;
        }

        private static async Task<int> GetMembersCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Admin.Server.SendAsync(new GetDatabaseTopologyOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Members.Count;
        }
    }
}
