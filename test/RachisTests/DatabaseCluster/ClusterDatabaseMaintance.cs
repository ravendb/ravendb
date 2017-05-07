using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Xunit;

namespace RachisTests.DatabaseCluster
{
    public class ClusterDatabaseMaintance : ReplicationTestsBase
    {
        [Fact]
        public async Task DemoteOnServerDown()
        {
            var clusterSize = 3;
            var databaseName = "DemoteOnServerDown";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize,true,0);
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                DefaultDatabase = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.Members.Count);
                Servers[1].Dispose();

                WaitForValue(() => store.Admin.Server.Send(new GetDatabaseTopologyOperation(databaseName)).Members.Count, clusterSize - 1);
                WaitForValue(() => store.Admin.Server.Send(new GetDatabaseTopologyOperation(databaseName)).Promotables.Count, 1);
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
                DefaultDatabase = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var createRes = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc));

                // TODO: replace when RavenDB- is done. from here
                var member = createRes.Topology.Members.Single();
                var dbServer = Servers.Single(s => s.ServerStore.NodeTag == member.NodeTag);
                await dbServer.ServerStore.Cluster.WaitForIndexNotification(createRes.ETag ?? 0);
                await dbServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);

                using (var dbStore = new DocumentStore
                {
                    Url = dbServer.WebUrls[0],
                    DefaultDatabase = databaseName
                }.Initialize())
                {
                    using (var session = dbStore.OpenAsyncSession())
                    {
                        await session.StoreAsync(new ReplicationBasicTests.User { Name = "Karmel" }, "users/1");
                        await session.SaveChangesAsync();
                    }
                }
                // to here
                var res = await store.Admin.Server.SendAsync(new AddDatabaseOperation(databaseName));

                Assert.Equal(1, res.Topology.Members.Count);
                Assert.Equal(1, res.Topology.Promotables.Count);
                
                WaitForValue(() => store.Admin.Server.Send(new GetDatabaseTopologyOperation(databaseName)).Members.Count, 2);
                WaitForValue(() => store.Admin.Server.Send(new GetDatabaseTopologyOperation(databaseName)).Promotables.Count, 0);


            }
        }

        [Fact]
        public async Task NoCrashOnLeaderChange()
        {
            var clusterSize = 3;
            var databaseName = "NoCrashOnLeaderChange";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, true, 0);
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                DefaultDatabase = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var res = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc,clusterSize));
                Assert.Equal(3, res.Topology.Members.Count);
            }
            leader.Dispose();
            Assert.True(WaitForValue(() => leader.ServerStore?.Disposed ?? true, true));

            using (var store = new DocumentStore()
            {
                Url = Servers[1].WebUrls[0],
                DefaultDatabase = databaseName
            }.Initialize())
            {
                Assert.Equal(2,WaitForValue(() => store.Admin.Server.Send(new GetDatabaseTopologyOperation(databaseName)).Members.Count, 2));
                Assert.Equal(1,WaitForValue(() => store.Admin.Server.Send(new GetDatabaseTopologyOperation(databaseName)).Promotables.Count, 1));
            }
        }
    }
}
