using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server.Documents;
using Raven.Server.Rachis;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Rachis
{
    
    public class Cluster: ClusterTestBase
    {
        [Fact]
        public async Task CanCreateDatabaseWithReplicationFactorLowerThanTheClusterSize()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);            
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
            }
            int numberOfInstances = 0;
            foreach (var server in Servers.Where(s => databaseResult.Topology.RelevantFor(s.ServerStore.NodeTag)))
            {                
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.ETag ?? 0);
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

        [Fact]
        public async Task CanDeleteDatabaseFromASpecificNodeInTheCluster()
        {
            NoTimeouts();
            var leader = await CreateRaftClusterAndGetLeader(3);
            DeleteDatabaseResult deleteResult;
            var databaseName = "test";
            var replicationFactor = 2;
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                DefaultDatabase = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                var databaseResult = store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));
                foreach (var server in Servers)
                {
                    await server.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.Equal, databaseResult.ETag??0);
                }
                var serverTagToBeDeleted = databaseResult.Topology.Members.First();
                deleteResult = store.Admin.Server.Send(new DeleteDatabaseOperation(databaseName, hardDelete:true,fromNode: serverTagToBeDeleted));
            }
            int numberOfInstances = 0;
            foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(deleteResult.ETag);
                try
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                    numberOfInstances++;
                }
                catch
                {
                    
                }
            }
            Assert.True(numberOfInstances == replicationFactor - 1, $"Expected replicationFactor={replicationFactor - 1} but got {numberOfInstances}");
        }
    }
}
