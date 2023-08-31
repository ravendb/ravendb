using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Cluster
{
    public class ClusterObserverTests : ReplicationTestBase
    {
        public ClusterObserverTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Move_To_Rehab_Grace_Time_Keeps_The_Priority_Order()
        {
            const int clusterSize = 3;
            var settings = new Dictionary<string, string>()
            {
                [RavenConfiguration.GetKey(x => x.Cluster.ElectionTimeout)] = 300.ToString(),
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "60",
                [RavenConfiguration.GetKey(x => x.Cluster.RotatePreferredNodeGraceTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Replication.ReplicationMinimalHeartbeat)] = "15",
            };

            var cluster = await CreateRaftCluster(clusterSize, false, 0, watcherCluster: true, customSettings: settings);
            var order = new List<string> { "A", "B", "C" };

            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = clusterSize,
                ModifyDatabaseRecord = x => x.Topology = new DatabaseTopology
                {
                    Members = order,
                    ReplicationFactor = 3,
                    PriorityOrder = order
                }
            }))
            {
                var tcs = new TaskCompletionSource<DocumentDatabase>();

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
                        tcs.SetCanceled();
                }))
                {
                    var t = preferred.ServerStore.DatabasesLandlord.DatabasesCache.ForTestingPurposesOnly().Replace(databaseName, tcs.Task);
                    t.Result.Dispose();

                    Assert.True(await WaitForValueAsync(async () =>
                    {
                        record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                        return record.Topology.Members[0] != preferred.ServerStore.NodeTag;
                    }, true));

                    val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 0);
                    Assert.Equal(0, val);
                    val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize);
                    Assert.Equal(clusterSize, val);
                }

                val = await WaitForValueAsync(async () => await GetRehabCount(store, databaseName), 0);
                Assert.Equal(0, val);
                val = await WaitForValueAsync(async () => await GetMembersCount(store, databaseName), clusterSize);
                Assert.Equal(clusterSize, val);

                await WaitForValueAsync(async () =>
                {
                    var r = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return r.Topology.Members.SequenceEqual(order);
                }, true);

                record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                Assert.Equal(order, record.Topology.Members);
            }
        }
    }
}
