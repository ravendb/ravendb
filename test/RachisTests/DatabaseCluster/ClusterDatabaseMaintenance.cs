using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
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
            var leader = await CreateRaftClusterAndGetLeader(clusterSize,true,0);
            using (var store = new DocumentStore
            {
                Url = leader.WebUrls[0],
                Database = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
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
            var leader = await CreateRaftClusterAndGetLeader(clusterSize,true,0);
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                Database = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var createRes = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc));

                var member = createRes.Topology.Members.Single();
                var dbServer = Servers.Single(s => s.ServerStore.NodeTag == member.NodeTag);
                await dbServer.ServerStore.Cluster.WaitForIndexNotification(createRes.ETag ?? 0);
                await dbServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);

                using (var dbStore = new DocumentStore
                {
                    Url = dbServer.WebUrls[0],
                    Database = databaseName
                }.Initialize())
                {
                    using (var session = dbStore.OpenAsyncSession())
                    {
                        await session.StoreAsync(new ReplicationBasicTests.User { Name = "Karmel" }, "users/1");
                        await session.SaveChangesAsync();
                    }
                }
                var res = await store.Admin.Server.SendAsync(new AddDatabaseNodeOperation(databaseName));
                Assert.Equal(1, res.Topology.Members.Count);
                Assert.Equal(1, res.Topology.Promotables.Count);

                await WaitForRaftIndexToBeAppliedInCluster(res.ETag ?? 0, TimeSpan.FromSeconds(5));
                await WaitForDocumentInClusterAsync<ReplicationBasicTests.User>(res.Topology, "users/1", u => u.Name == "Karmel",TimeSpan.FromSeconds(10));
                                
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
                Url = leader.WebUrls[0],
                Database = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var res = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc,clusterSize));
                await WaitForRaftIndexToBeAppliedInCluster(res.ETag ?? 0, TimeSpan.FromSeconds(5));
                Assert.Equal(3, res.Topology.Members.Count);
            }

            leader.Dispose();
            
            using (var store = new DocumentStore()
            {
                Url = Servers[1].WebUrls[0],
                Database = databaseName
            }.Initialize())
            {
                var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2);
                Assert.Equal(2, val);
                val = await WaitForValueAsync(async () => await GetPromotableCount(store, databaseName), 1);
                Assert.Equal(1, val);
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
