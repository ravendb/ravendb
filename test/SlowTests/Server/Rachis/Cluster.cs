using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server.Rachis;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Rachis
{
    
    public class Cluster: ClusterTestBase
    {
        [Fact]
        public async Task test()
        {
            NoTimeouts();
            var leader = await CreateRaftClusterAndGetLeader(3);            
            CreateDatabaseResult databaseResult;
            var replicationFactor = 2;
            using (var store = new DocumentStore()
            {
                Url = leader.WebUrls[0],
                DefaultDatabase = "test"
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument("test");
                databaseResult = store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));
            }
            int numberOfInstances = 0;
            foreach (var server in Servers.Where(s => databaseResult.Topology.RelevantFor(s.ServerStore.NodeTag)))
            {                
                await server.ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.Equal, databaseResult.ETag ?? 0);
                using (var store = new DocumentStore() { Url = server.WebUrls[0] , DefaultDatabase = "test" }.Initialize())
                {
                    if (store.Admin.Server.Send(new GetDatabaseNamesOperation(0, 100)).Contains("test"))
                    {
                        numberOfInstances++;
                    }
                }
            }
            Assert.True(numberOfInstances == replicationFactor, $"Expected replicationFactor={replicationFactor} but got {numberOfInstances}");
        }
    }
}
