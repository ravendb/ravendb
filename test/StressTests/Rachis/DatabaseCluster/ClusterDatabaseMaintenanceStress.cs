using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Session;
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
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "10",
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
                var tcs = new TaskCompletionSource<DocumentDatabase>();

                var databaseName = store.Database;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "users/1", _ => true, TimeSpan.FromSeconds(5)));
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var preferred = Servers.Single(s => s.ServerStore.NodeTag == record.Topology.Members[0]);

                int val;
                using (new DisposableAction(() =>
                {
                    preferred.ServerStore.DatabasesLandlord.DatabasesCache.TryRemove(databaseName, out var t);
                    if (t == tcs.Task)
                        tcs.SetCanceled();
                }))
                {
                    var t = preferred.ServerStore.DatabasesLandlord.DatabasesCache.Replace(databaseName, tcs.Task);
                    t.Result.Dispose();

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
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "10",
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
                var tcs = new TaskCompletionSource<DocumentDatabase>();

                var databaseName = store.Database;
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Karmel" }, "users/1");
                    session.SaveChanges();

                    Assert.True(await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "users/1", _ => true, TimeSpan.FromSeconds(5)));
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var fixedOrder = record.Topology.AllNodes.ToList();
                await store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(store.Database, fixedOrder, fixedTopology: true));

                var preferred = Servers.Single(s => s.ServerStore.NodeTag == record.Topology.Members[0]);

                int val;
                using (new DisposableAction(() =>
                {
                    preferred.ServerStore.DatabasesLandlord.DatabasesCache.TryRemove(databaseName, out var t);
                    if (t == tcs.Task)
                        tcs.SetCanceled();
                }))
                {
                    var t = preferred.ServerStore.DatabasesLandlord.DatabasesCache.Replace(databaseName, tcs.Task);
                    t.Result.Dispose();

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
    }
}
