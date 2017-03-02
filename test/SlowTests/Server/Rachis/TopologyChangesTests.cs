using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;

namespace SlowTests.Server.Rachis
{
    public class TopologyChangesTests : RachisConsensusTestBase
    {
        [Fact(Skip = "Nodes don't seem to elect a new leader")]
        public async Task CanEnforceTopologyOnOldLeader()
        {
            var leader = await CreateNetworkAndGetLeader(3);
            var followers = GetFollowers();
            DisconnectFromNode(leader);
            var newServer = SetupServer();
            await leader.AddToClusterAsync(newServer.Url);
            await newServer.WaitForTopology(Leader.TopologyModification.Promotable);
            var newLeader = WaitForAnyToBecomeLeader(followers);
            Assert.NotNull(newLeader);
            ReconnectToNode(leader);
            await leader.WaitForState(RachisConsensus.State.Follower);
            TransactionOperationContext context;
            using (leader.ContextPool.AllocateOperationContext(out context))
            {
                var topology = leader.GetTopology(context);
                Assert.False(topology.Voters.Contains(newServer.Url) == false &&
                             topology.Promotables.Contains(newServer.Url) == false &&
                             topology.NonVotingMembers.Contains(newServer.Url) == false);
            }
        }
    }
}
