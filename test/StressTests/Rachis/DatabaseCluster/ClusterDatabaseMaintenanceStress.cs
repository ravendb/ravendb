using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Rachis.DatabaseCluster
{
    public class ClusterDatabaseMaintenanceStress : ReplicationTestBase
    {
        public ClusterDatabaseMaintenanceStress(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
            public string Email { get; set; }
            public int Age { get; set; }
        }

        [Fact]
        public async Task MoveLoadingNodeToLast()
        {
            var clusterSize = 3;
            var settings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = 300.ToString(),
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "5",
                [RavenConfiguration.GetKey(x => x.Cluster.RotatePreferredNodeGraceTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "15",
            };

            var cluster = await CreateRaftCluster(clusterSize, false, 0, watcherCluster: true, customSettings: settings);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = clusterSize
            }))
            {
                var tcs = new TaskCompletionSource<DocumentDatabase>(TaskCreationOptions.RunContinuationsAsynchronously);

                var databaseName = store.Database;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, databaseName, "users/1", _ => true, TimeSpan.FromSeconds(5)));
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var preferred = Servers.Single(s => s.ServerStore.NodeTag == record.Topology.Members[0]);

                int val;
                using (new DisposableAction(() =>
                {
                    if (preferred.ServerStore.DatabasesLandlord.DatabasesCache.TryRemove(databaseName, tcs.Task))
                        tcs.TrySetCanceled();
                }))
                {
                    var t = await preferred.ServerStore.DatabasesLandlord.DatabasesCache.ForTestingPurposesOnly().Replace(databaseName, tcs.Task);
                    t.Dispose();

                    Assert.True(await WaitForValueAsync(async () =>
                    {
                        record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                        return record.Topology.Members[0] != preferred.ServerStore.NodeTag;
                    }, true));

                    val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 1);
                    Assert.Equal(1, val);
                    val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize - 1);
                    Assert.Equal(clusterSize - 1, val);
                }

                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 0);
                Assert.Equal(0, val);
                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize);
                Assert.Equal(clusterSize, val);
            }
        }

        [Fact]
        public async Task MoveLoadingNodeToLastAndRestoreToFixedOrder()
        {
            var clusterSize = 3;
            var settings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = 300.ToString(),
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "5",
                [RavenConfiguration.GetKey(x => x.Cluster.RotatePreferredNodeGraceTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "15",
            };

            var cluster = await CreateRaftCluster(clusterSize, false, 0, watcherCluster: true, customSettings: settings);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = clusterSize
            }))
            {
                var tcs = new TaskCompletionSource<DocumentDatabase>(TaskCreationOptions.RunContinuationsAsynchronously);

                var databaseName = store.Database;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>(cluster.Nodes, databaseName, "users/1", _ => true, TimeSpan.FromSeconds(5)));
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var fixedOrder = record.Topology.AllNodes.ToList();
                await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(store.Database, fixedOrder, fixedTopology: true));

                var preferred = Servers.Single(s => s.ServerStore.NodeTag == record.Topology.Members[0]);

                int val;
                using (new DisposableAction(() =>
                {
                    if (preferred.ServerStore.DatabasesLandlord.DatabasesCache.TryRemove(databaseName, tcs.Task))
                        tcs.TrySetCanceled();
                }))
                {
                    var t = await preferred.ServerStore.DatabasesLandlord.DatabasesCache.ForTestingPurposesOnly().Replace(databaseName, tcs.Task);
                    t.Dispose();

                    Assert.True(await WaitForValueAsync(async () =>
                    {
                        record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                        return record.Topology.Members[0] != preferred.ServerStore.NodeTag;
                    }, true));

                    val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 1);
                    Assert.Equal(1, val);
                    val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize - 1);
                    Assert.Equal(clusterSize - 1, val);
                }

                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 0);
                Assert.Equal(0, val);
                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), 2);
                Assert.Equal(clusterSize, val);

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                Assert.Equal(fixedOrder, record.Topology.Members);
            }
        }

        [RavenFact(RavenTestCategory.Cluster)]
        public async Task TakingIntoAccountMoveToRehabGraceTimeConfiguration()
        {
            const int moveToRehabGraceTimeInSec = 60;
            var databaseName = GetDatabaseName();
            var settings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = moveToRehabGraceTimeInSec.ToString(),
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.AddReplicaTimeout)] = "1"
            };

            var (servers, leader) = await CreateRaftCluster(numberOfNodes: 3, shouldRunInMemory: false, customSettings: settings);
            using (var leaderStore = new DocumentStore { Urls = new[] { leader.WebUrl }, Database = databaseName, }.Initialize())
            {
                var topology = new DatabaseTopology { Members = new List<string> { "A", "B", "C" }, DynamicNodesDistribution = true };

                var (index, _) = await CreateDatabaseInCluster(new DatabaseRecord { DatabaseName = databaseName, Topology = topology }, replicationFactor: 3,
                    leader.WebUrl);
                await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(index, TimeSpan.FromSeconds(30));

                using (var session = leaderStore.OpenSession())
                {
                    session.Store(new User(), "users/1");
                    session.SaveChanges();
                }
                var databaseTopology = (await leaderStore.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(databaseName))).Topology;
                Assert.Equal(3, databaseTopology.AllNodes.Count());
                Assert.Equal(0, databaseTopology.Promotables.Count);
                Assert.True(await WaitForDocumentInClusterAsync<User>(topology, databaseName, "users/1", null, TimeSpan.FromSeconds(30)));

                var serverA = servers.Single(s => s.ServerStore.NodeTag == "A");

                await Task.Delay(TimeSpan.FromSeconds(moveToRehabGraceTimeInSec)); // we need to take into account database uptime

                var sw = Stopwatch.StartNew();
                await DisposeServerAndWaitForFinishOfDisposalAsync(serverA);

                var disposeTime = sw.Elapsed;

                var count = await WaitForValueAsync(async () => await GetRehabCount(leaderStore, databaseName),
                    expectedVal: 1,
                    timeout: (int)TimeSpan.FromSeconds(moveToRehabGraceTimeInSec * 2).TotalMilliseconds);

                sw.Stop();

                var acceptableDeviation = TimeSpan.FromSeconds(1);
                Assert.Equal(1, count);
                Assert.True(sw.Elapsed > TimeSpan.FromSeconds(moveToRehabGraceTimeInSec) - disposeTime - acceptableDeviation,
                    userMessage: $"The grace period was not considered and node 'A' went into rehab after {sw.Elapsed}, " +
                                 $"but grace period is '{moveToRehabGraceTimeInSec}' sec (disposing of the node took '{disposeTime}').");
            }
        }

    }
}
