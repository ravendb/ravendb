using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20202 : ClusterTestBase
{
    public RavenDB_20202(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task ShouldGettingBackToTheNodeAfterExperiencingTimeoutAndTopologyUpdateSimultaneously()
    {
        const string databaseName = "testDb";
        const int clusterSize = 3;
        var timeOut = TimeSpan.FromSeconds(5);

        var (nodes, leaderServer) = await CreateRaftCluster(clusterSize);
        await CreateDatabaseInCluster(databaseName, 3, leaderServer.WebUrl);

        using (var store = new DocumentStore
        {
            Urls = new[] { leaderServer.WebUrl },
            Database = databaseName,
            Conventions = new DocumentConventions { RequestTimeout = timeOut, }
        }.Initialize())
        {
            List<string> originalNodesOrder = new() { "A", "B", "C" };
            await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(store.Database, originalNodesOrder));

            var databaseRecordWithEtag = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            Assert.Equal(originalNodesOrder, databaseRecordWithEtag.Topology.Members);

            // We'll hold database settings load and health check executions for nodes 'A' and 'B' only
            foreach (var node in nodes.Where(node => node.ServerStore.NodeTag is "A" or "B"))
            {
                var db = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                Assert.NotNull(db);
                var forTestingPurposes = db.ForTestingPurposesOnly();

                forTestingPurposes.DatabaseRecordLoadHold = new ManualResetEvent(false);
                forTestingPurposes.HealthCheckHold = new ManualResetEvent(false);
            }

            // We'll get TimeOutException for the first two nodes and be prepared to create
            // a specific condition while experiencing a timeout and topology update simultaneously.
            store.Maintenance.Send(new GetDatabaseSettingsOperation(store.Database));

            // Let's force topology update
            // Set a new order of nodes
            var newNodesOrder = new List<string>(originalNodesOrder);
            newNodesOrder.Shuffle();
            await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(store.Database, newNodesOrder));
            databaseRecordWithEtag = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            Assert.Equal(newNodesOrder, databaseRecordWithEtag.Topology.Members);

            //And revert to the original nodes order back
            await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(store.Database, originalNodesOrder));
            databaseRecordWithEtag = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));
            Assert.Equal(originalNodesOrder, databaseRecordWithEtag.Topology.Members);

            // We'll hold database settings load for the last node 'C' (but health check still allowed to execute)
            var nodeC = nodes.Single(node => node.ServerStore.NodeTag is "C");
            var nodeCdb = await nodeC.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
            Assert.NotNull(nodeCdb);
            nodeCdb.ForTestingPurposesOnly().DatabaseRecordLoadHold = new ManualResetEvent(false);

            var error = Assert.Throws<AllTopologyNodesDownException>(() => store.Maintenance.Send(new GetDatabaseSettingsOperation(store.Database)));
            Assert.Contains($"`GET /databases/{databaseName}/admin/record` to all configured nodes in the topology, none of the attempt succeeded.", error.Message);

            // Now we'll allow all waiting threads to proceed
            foreach (var node in nodes)
            {
                var db = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                Assert.NotNull(db);
                var forTestingPurposes = db.ForTestingPurposesOnly();
                forTestingPurposes.DatabaseRecordLoadHold?.Set();
                forTestingPurposes.HealthCheckHold?.Set();
            }

            // Wait for timers to execute callbacks; expect no timers after it
            WaitForValue(() =>
            {
                // Assert that there are no health check timers for failed nodes
                var failedNodesTimers = store.GetRequestExecutor().ForTestingPurposesOnly().FailedNodesTimers;
                return failedNodesTimers.Count;
            }, 
                expectedVal: 0, 
                timeout: Convert.ToInt32(TimeSpan.FromMinutes(1).TotalMilliseconds), 
                interval: Convert.ToInt32(TimeSpan.FromSeconds(1).TotalMilliseconds));

            // Assert that there are no failures in the node selector state
            var nodeSelectorFailures = store.GetRequestExecutor().ForTestingPurposesOnly().NodeSelectorFailures;
            Assert.Equal(new[] { 0, 0, 0 }, nodeSelectorFailures);

            // Assert that we're back to the first node 
            var preferredNode = store.GetRequestExecutor().ForTestingPurposesOnly().PreferredNode;
            Assert.Equal(0, preferredNode.Index);
            Assert.Equal("A", preferredNode.Node.ClusterTag);
        }
    }
}
