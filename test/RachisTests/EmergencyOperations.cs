using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    public class EmergencyOperations : ClusterTestBase
    {
        public EmergencyOperations(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task LeaderCanCecedeFromClusterAndNewLeaderWillBeElected()
        {
            var (nodes, leader) = await CreateRaftCluster(3);
            ClusterTopology old, @new;
            old = leader.ServerStore.GetClusterTopology();
            await leader.ServerStore.Engine.HardResetToNewClusterAsync("A");
            await leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None);
            @new = leader.ServerStore.GetClusterTopology();
            Assert.NotEqual(old.TopologyId, @new.TopologyId);
            var leaderSelectedTasks = new List<Task>();
            var followers = nodes.Where(n => n != leader);
            foreach (var server in followers)
            {
                leaderSelectedTasks.Add(server.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None));
            }
            Assert.True(await Task.WhenAny(leaderSelectedTasks).WaitWithoutExceptionAsync(TimeSpan.FromSeconds(10)), "New leader was not elected after old leader left the cluster.");
        }

        [Fact]
        public async Task FollowerCanCecedeFromCluster()
        {
            var clusterSize = 3;
            await CreateRaftCluster(clusterSize);
            var follower = Servers.First(x => x.ServerStore.CurrentRachisState == RachisState.Follower);
            ClusterTopology old, @new;
            old = follower.ServerStore.GetClusterTopology();
            new AdminJsConsole(follower, null).ApplyScript(new AdminJsScript(@"server.ServerStore.Engine.HardResetToNewCluster('A');"));
            await follower.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None);
            @new = follower.ServerStore.GetClusterTopology();
            Assert.NotEqual(old.TopologyId, @new.TopologyId);
        }

        [Fact]
        public async Task ClusterRecreationShouldKeepClusterTransactionAndDatabaseTopologyIds()
        {
            var databaseName = GetDatabaseName();
            var (_, leader) = await CreateRaftCluster(2, shouldRunInMemory: false);
            await CreateDatabaseInCluster(databaseName, 2, leader.WebUrl);
            
            var old = GetDatabaseTopology();
            leader.ServerStore.Engine.HardResetToNewCluster();
            await leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None);

            var @new = GetDatabaseTopology();
            Assert.Equal(old.ClusterTransactionIdBase64, @new.ClusterTransactionIdBase64);
            Assert.Equal(old.DatabaseTopologyIdBase64, @new.DatabaseTopologyIdBase64);
            
            DatabaseTopology GetDatabaseTopology()
            {
                using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                    return leader.ServerStore.Cluster.ReadDatabaseTopology(ctx, databaseName);
            }
        }
    }
}
