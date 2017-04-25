using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Rachis
{
    
    public class Cluster: ClusterTestBase
    {
        [Fact]
        public async Task CanCreateAddAndDeleteDatabaseFromNodes()
        {
            var clusterSize = 3;
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            CreateDatabaseResult databaseResult;
            var replicationFactor = 2;
            var databaseName = "test";
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                DefaultDatabase = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                databaseResult = store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));

                int numberOfInstances = 0;
                await AssertNumberOfNodesContainingDatabase(databaseResult.ETag??0, databaseName, numberOfInstances, replicationFactor);
                databaseResult = store.Admin.Server.Send(new AddDatabaseOperation(databaseName));
                Assert.Equal(databaseResult.Topology.AllNodes.Count(), ++replicationFactor);
                numberOfInstances = 0;
                await AssertNumberOfNodesContainingDatabase(databaseResult.ETag ?? 0, databaseName, numberOfInstances, replicationFactor);
                DeleteDatabaseResult deleteResult;
                var startReplicationFactor = replicationFactor;
                while (replicationFactor>0)
                {
                    var serverTagToBeDeleted = databaseResult.Topology.Members[startReplicationFactor-replicationFactor].NodeTag;
                    replicationFactor--;
                    deleteResult = store.Admin.Server.Send(new DeleteDatabaseOperation(databaseName, hardDelete: true, fromNode: serverTagToBeDeleted));
                    //The +1 is for NotifyLeaderAboutRemoval
                    await AssertNumberOfNodesContainingDatabase(deleteResult.ETag + 1, databaseName, numberOfInstances, replicationFactor);
                }
                TransactionOperationContext context;
                using (leader.ServerStore.ContextPool.AllocateOperationContext(out context))
                using (context.OpenReadTransaction())
                {
                    Assert.Null(leader.ServerStore.Cluster.ReadDatabase(context, databaseName));                    
                }
            }
        }

        private async Task AssertNumberOfNodesContainingDatabase(long eTag, string databaseName, int numberOfInstances, int replicationFactor)
        {
            foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(eTag);
                try
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                    numberOfInstances++;
                }
                catch
                {
                }
            }
            Assert.True(numberOfInstances == replicationFactor, $"Expected replicationFactor={replicationFactor} but got {numberOfInstances}");
        }

    }
}
