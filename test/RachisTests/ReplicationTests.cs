using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server.Rachis;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests
{
    public class ReplicationTests : ReplicationBasicTests
    {
        [Fact]
        public async Task EnsureDocumentsReplication()
        {
            var clusterSize = 5;
            var databaseName = "ReplicationTestDB";
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                DefaultDatabase = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var databaseResult = await store.Admin.Server.SendAsync(new CreateDatabaseOperation(doc, clusterSize));
                Assert.Equal(clusterSize, databaseResult.Topology.AllReplicationNodes.Count());
                foreach (var server in Servers)
                {
                    await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.ETag ?? -1);
                }
                foreach (var server in Servers)
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                }
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name  = "Karmel"},"users/1");
                    await session.SaveChangesAsync();
                }
                Assert.True(await WaitForDocumentInClusterAsync<User>(
                    databaseResult.Topology,
                    "users/1",
                    u => u.Name.Equals("Karmel"),
                    TimeSpan.FromSeconds(clusterSize + 5)));
                await Task.Delay(TimeSpan.FromMilliseconds(500));
                var stats = leader.ServerStore.ClusterStats();
                Assert.NotEmpty(stats);
                foreach (var server in databaseResult.Topology.AllReplicationNodes)
                {
                    Assert.Equal(1,stats[server.NodeTag].LastReport[databaseName].LastDocumentChangeVector.Length);
                }
            }
        }      
    }
}
