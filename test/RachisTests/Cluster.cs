using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests
{

    public class Cluster : ClusterTestBase
    {
        private static async Task<int> GetMembersCount(IDocumentStore store, string databaseName)
        {
            var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            if (res == null)
            {
                return -1;
            }
            return res.Topology.Members.Count;
        }

        [Fact]
        public async Task PutUniqeValueToDifferentNode()
        {
            var clusterSize = 3;
            var databaseName = GetDatabaseName();
            var leader = await CreateRaftClusterAndGetLeader(clusterSize, false, 0);
            using (var store = new DocumentStore
            {
                Urls = new[] { Servers[1].WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName)
                {
                    Topology = new DatabaseTopology
                    {
                        Members = new List<string>
                        {
                            "B"
                        }
                    }
                };

                var res = await store.Maintenance.Server.SendAsync(new CreateDatabaseOperation(doc));
                Assert.NotEqual(res.Topology.Members.First(), leader.ServerStore.NodeTag);

                await store.Operations.SendAsync(new PutCompareExchangeValueOperation<string>("test", "Karmel", 0));
                var val = await store.Operations.SendAsync(new GetCompareExchangeValueOperation<string>("test"));
                Assert.Equal("Karmel", val.Value);
            }
        }

        [Fact]
        public async Task CanCreateAddAndDeleteDatabaseFromNodes()
        {
            var clusterSize = 3;
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            var replicationFactor = 2;
            var databaseName = GetDatabaseName();
            using (var store = new DocumentStore()
            {
                Urls = new[] { leader.WebUrl },
                Database = databaseName
            }.Initialize())
            {
                var doc = new DatabaseRecord(databaseName);
                var databaseResult = store.Maintenance.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));

                int numberOfInstances = 0;
                await AssertNumberOfNodesContainingDatabase(databaseResult.RaftCommandIndex, databaseName, numberOfInstances, replicationFactor);
                databaseResult = store.Maintenance.Server.Send(new AddDatabaseNodeOperation(databaseName));
                Assert.Equal(databaseResult.Topology.AllNodes.Count(), ++replicationFactor);
                numberOfInstances = 0;
                await AssertNumberOfNodesContainingDatabase(databaseResult.RaftCommandIndex, databaseName, numberOfInstances, replicationFactor);
                while (replicationFactor > 0)
                {
                    var val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), replicationFactor);
                    Assert.Equal(replicationFactor, val);
                    var res = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));

                    var serverTagToBeDeleted = res.Topology.Members[0];
                    replicationFactor--;
                    var deleteResult = store.Maintenance.Server.Send(new DeleteDatabasesOperation(databaseName, hardDelete: true, fromNode: serverTagToBeDeleted, timeToWaitForConfirmation: TimeSpan.FromSeconds(30)));
                    await WaitForDatabaseToBeDeleted(store,databaseName,TimeSpan.FromSeconds(30));
                    await AssertNumberOfNodesContainingDatabase(deleteResult.RaftCommandIndex, databaseName, numberOfInstances, replicationFactor);
                }
                using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    Assert.Null(leader.ServerStore.Cluster.ReadDatabase(context, databaseName));
                }
            }
        }

        private async Task<bool> WaitForDatabaseToBeDeleted(IDocumentStore store, string databaseName,TimeSpan timeout)
        {
            var pollingInterval = timeout.TotalSeconds<1? timeout:TimeSpan.FromSeconds(1);
            var sw = Stopwatch.StartNew();
            while (true)
            {
                var delayTask = Task.Delay(pollingInterval);
                var dbTask = store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
                var doneTask = await Task.WhenAny(dbTask, delayTask);
                if (doneTask == delayTask)
                {
                    if (sw.Elapsed > timeout)
                    {
                        return false;
                    }
                    continue;
                }
                var dbRecord = dbTask.Result;
                if (dbRecord == null || dbRecord.DeletionInProgress == null || dbRecord.DeletionInProgress.Count == 0)
                {
                    return true;
                }
            }
        }

        private async Task AssertNumberOfNodesContainingDatabase(long eTag, string databaseName, int numberOfInstances, int replicationFactor)
        {
            await Task.Delay(500);

            foreach (var server in Servers)
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(eTag);
                try
                {
                    if (server.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(databaseName, out var _))
                        numberOfInstances++;
                }
                catch
                {
                    // ignored
                }
            }
            Assert.True(numberOfInstances == replicationFactor, $"Expected replicationFactor={replicationFactor} but got {numberOfInstances}");
        }

    }
}
