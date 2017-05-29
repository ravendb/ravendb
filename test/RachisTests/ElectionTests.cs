using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Rachis;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Rachis
{
    public class ElectionTests : RachisConsensusTestBase
    {
        [Fact]
        public async Task Follower_as_a_single_node_becomes_leader_automatically()
        {
            var node = SetupServer(true);
            var nodeCurrentState = node.CurrentState;
            Assert.True(nodeCurrentState == RachisConsensus.State.LeaderElect ||
                        nodeCurrentState == RachisConsensus.State.Leader);
            Assert.True(await node.WaitForState(RachisConsensus.State.Leader).WaitAsync(node.ElectionTimeout), "Node didn't become leader although he is alone in his cluster.");
        }

        /// <summary>
        /// This test checks a few things (I didn't want to have to repeat the same logic in multiple tests)
        /// 1) it checks that a new leader is elected when the old leader is cut-off the cluster
        /// 2) it checks that when the old leader joins the cluster he is a follower 
        /// 3) commands that the old leader sent are not committed and that the new leader is able to override the old leader's log
        /// </summary>
        /// <param name="numberOfNodes">The number of nodes in the cluster</param>
        /// <returns></returns>
        [Theory]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task OnNetworkDisconnectionANewLeaderIsElectedAfterReconnectOldLeaderStepsDownAndRollBackHisLog(int numberOfNodes)        
        {
            var firstLeader = await CreateNetworkAndGetLeader(numberOfNodes);
            var timeToWait = TimeSpan.FromMilliseconds(firstLeader.ElectionTimeout.TotalMilliseconds * 4);
            await IssueCommandsAndWaitForCommit(firstLeader, 3, "test", 1);
           
            DisconnectFromNode(firstLeader);
            List<Task> invalidCommands = IssueCommandsWithoutWaitingForCommits(firstLeader, 5, "test", 1);
            var followers = GetFollowers();
            List<Task> waitingList = new List<Task>();
            foreach (var follower in followers)
            {
                waitingList.Add(follower.WaitForState(RachisConsensus.State.Leader));
            }
            if (Log.IsInfoEnabled)
            {
                Log.Info("Started waiting for new leader");
            }
            var done = await Task.WhenAny(waitingList).WaitAsync(timeToWait);
            Assert.True(done, "Followers didn't become leaders although old leader can't communicate with the cluster");

            var newLeader = followers.First(f => f.CurrentState == RachisConsensus.State.Leader);
            var newLeaderLastIndex = await IssueCommandsAndWaitForCommit(newLeader, 3, "test", 1);
            ReconnectToNode(firstLeader);
            Assert.True(await firstLeader.WaitForState(RachisConsensus.State.Follower).WaitAsync(timeToWait), "Old leader didn't become follower after two election timeouts");
            var waitForCommitIndexChange = firstLeader.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newLeaderLastIndex);
            Assert.True(await waitForCommitIndexChange.WaitAsync(timeToWait), "Old leader didn't rollback his log to the new leader log");
            var leaderUrl = new HashSet<string>();
            foreach (var consensus in RachisConsensuses)
            {
                leaderUrl.Add(consensus.LeaderTag);
            }
            Assert.True(leaderUrl.Count == 1, "Not all nodes agree on the leader");
            foreach (var invalidCommand in invalidCommands)
            {
                Assert.True(invalidCommand.IsCompleted);
                Assert.NotEqual(TaskStatus.RanToCompletion, invalidCommand.Status);
            }
        }

    }
}
