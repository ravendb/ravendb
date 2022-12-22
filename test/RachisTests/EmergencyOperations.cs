using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server;
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
            Assert.True(await WaitForNotHavingPromotables(nodes)); //Waiting for everyone not to be promotable in everyone's topologies.
            ClusterTopology old, @new;
            old = GetServerTopology(leader);
            leader.ServerStore.Engine.HardResetToNewCluster("A");
            await leader.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None);
            @new = GetServerTopology(leader);
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
            old = GetServerTopology(follower);
            new AdminJsConsole(follower, null).ApplyScript(new AdminJsScript(@"server.ServerStore.Engine.HardResetToNewCluster('A');"));
            await follower.ServerStore.WaitForState(RachisState.Leader, CancellationToken.None);
            @new = GetServerTopology(follower);
            Assert.NotEqual(old.TopologyId, @new.TopologyId);
        }

        private static ClusterTopology GetServerTopology(RavenServer leader)
        {
            ClusterTopology old;
            using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                old = leader.ServerStore.GetClusterTopology(ctx);
            }

            return old;
        }

        private static async Task<bool> WaitForNotHavingPromotables(List<RavenServer> servers, long timeout = 15_000)
        {
            var tasks = new List<Task>();
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeout)
            {
                bool havePromotables = false;
                foreach (var server in servers)
                {
                    var t1 = GetServerTopology(server);
                    if (t1.Promotables.Count > 0)
                    {
                        havePromotables = true;
                        break;
                    }
                }

                if (havePromotables == false)
                {
                    return true;
                }

                await Task.Delay(200);
            }

            return false;

        }
    }
}
