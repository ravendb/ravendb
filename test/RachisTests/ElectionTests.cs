using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Tests.Infrastructure;
using Xunit;

namespace RachisTests
{
    public class ElectionTests : RachisConsensusTestBase
    {
        [Fact]
        public async Task Follower_as_a_single_node_becomes_leader_automatically()
        {
            var node = SetupServer(true);
            var nodeCurrentState = node.CurrentState;
            Assert.True(nodeCurrentState == RachisState.LeaderElect ||
                        nodeCurrentState == RachisState.Leader);
            var waitForState = node.WaitForState(RachisState.Leader, CancellationToken.None);

            var condition = await waitForState.WaitAsync(10 * node.ElectionTimeout);
            Assert.True(condition, $"Node is in state {node.CurrentState} and didn't become leader although he is alone in his cluster.");
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(5)]
        public async Task ForceStepDownAndElectNewLeader(int numberOfNodes)
        {
            var firstLeader = await CreateNetworkAndGetLeader(numberOfNodes);
            firstLeader.CurrentLeader.StepDown();
            Assert.True(await firstLeader.WaitForState(RachisState.Follower, CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(3)), "Old leader hasn't stepped down.");
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
            var timeToWait = TimeSpan.FromMilliseconds(firstLeader.ElectionTimeout.TotalMilliseconds * 4 * numberOfNodes); // was 'TotalMilliseconds * 4', changed to *8 for low end machines RavenDB-7263
            await IssueCommandsAndWaitForCommit(firstLeader, 3, "test", 1);

            DisconnectFromNode(firstLeader);
            List<Task> invalidCommands = IssueCommandsWithoutWaitingForCommits(firstLeader, 3, "test", 1);
            var followers = GetFollowers();
            List<Task> waitingList = new List<Task>();
            var currentTerm = 1L;
            while (true)
            {
                foreach (var follower in followers)
                {
                    waitingList.Add(follower.WaitForState(RachisState.Leader, CancellationToken.None));
                }
                if (Log.IsInfoEnabled)
                {
                    Log.Info("Started waiting for new leader");
                }
                var done = await Task.WhenAny(waitingList).WaitAsync(timeToWait);
                if (done)
                {
                    break;
                }
                var maxTerm = followers.Max(f => f.CurrentTerm);
                Assert.True(currentTerm + 1 < maxTerm, "Followers didn't become leaders although old leader can't communicate with the cluster");
                Assert.True(maxTerm < 10, "Followers were unable to elect a leader.");
                currentTerm = maxTerm;
                waitingList.Clear();
            }


            var newLeader = followers.First(f => f.CurrentState == RachisState.Leader);
            var newLeaderLastIndex = await IssueCommandsAndWaitForCommit(newLeader, 5, "test", 1);
            if (Log.IsInfoEnabled)
            {
                Log.Info("Reconnect old leader");
            }
            ReconnectToNode(firstLeader);
            Assert.True(await firstLeader.WaitForState(RachisState.Follower, CancellationToken.None).WaitAsync(timeToWait), "Old leader didn't become follower after two election timeouts");
            var waitForCommitIndexChange = firstLeader.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newLeaderLastIndex);
            Assert.True(await waitForCommitIndexChange.WaitAsync(timeToWait), "Old leader didn't rollback his log to the new leader log");
            Assert.Equal(numberOfNodes, RachisConsensuses.Count);
            var leaderUrl = new HashSet<string>();
            foreach (var consensus in RachisConsensuses)
            {
                if (consensus.Tag != consensus.LeaderTag)
                {
                    Assert.True(await consensus.WaitForState(RachisState.Follower, CancellationToken.None).WaitAsync(1000));
                }
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
