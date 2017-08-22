using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lucene.Net.Support;
using Raven.Client.Http;
using Raven.Server;
using Raven.Server.Documents.Patch;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests
{
    public class EmergencyOperations : ClusterTestBase
    {
        [Fact(Skip = "RavenDB-8265")]
        public async Task LeaderCanCecedeFromClusterAndNewLeaderWillBeElected()
        {
            var clusterSize = 3;
            var leader = await CreateRaftClusterAndGetLeader(clusterSize);
            ClusterTopology old, @new;
            old = GetServerTopology(leader);
            new AdminJsConsole(leader,null).ApplyScript(new AdminJsScript
            (
               @"server.ServerStore.SecedeFromCluster();"
            ));
            await leader.ServerStore.WaitForState(RachisConsensus.State.Leader);
            @new = GetServerTopology(leader);
            Assert.NotEqual(old.TopologyId,@new.TopologyId);
            List<Task<RavenServer>> leaderSelectedTasks = new List<Task<RavenServer>>();
            foreach (var server in Servers)
            {
                if(server == leader)
                    continue;
                leaderSelectedTasks.Add(server.ServerStore.WaitForState(RachisConsensus.State.Leader).ContinueWith(_=>server));
            }
            Assert.True(await Task.WhenAny(leaderSelectedTasks).WaitAsync(TimeSpan.FromSeconds(2)),"New leader was not elected after old leader left the cluster.");            
        }

        [Fact(Skip = "RavenDB-8265")]
        public async Task FollowerCanCecedeFromCluster()
        {
            var clusterSize = 3;
            await CreateRaftClusterAndGetLeader(clusterSize);
            var follower = Servers.First(x => x.ServerStore.CurrentState == RachisConsensus.State.Follower);
            ClusterTopology old, @new;
            old = GetServerTopology(follower);
            new AdminJsConsole(follower, null).ApplyScript(new AdminJsScript(@"server.ServerStore.SecedeFromCluster();"));
            await follower.ServerStore.WaitForState(RachisConsensus.State.Leader);
            @new = GetServerTopology(follower);
            Assert.NotEqual(old.TopologyId, @new.TopologyId);            
        }

        private static ClusterTopology GetServerTopology(RavenServer leader)
        {
            ClusterTopology old;
            using (leader.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using(ctx.OpenReadTransaction())
            {
                old = leader.ServerStore.GetClusterTopology(ctx);
            }

            return old;
        }
    }
}
