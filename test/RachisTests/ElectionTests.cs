using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Cluster;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
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
            var timeToWait = TimeSpan.FromMilliseconds(1000 * numberOfNodes);
            await IssueCommandsAndWaitForCommit(3, "test", 1);
            var currentTerm = firstLeader.CurrentTerm;
            DisconnectFromNode(firstLeader);
            List<Task> invalidCommands = IssueCommandsWithoutWaitingForCommits(firstLeader, 3, "test", 1);
            var followers = GetFollowers();
            List<Task> waitingList = new List<Task>();
            while (true)
            {
                using (var cts = new CancellationTokenSource())
                {
                    foreach (var follower in followers)
                    {
                        waitingList.Add(follower.WaitForState(RachisState.Leader, cts.Token));
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
                    Assert.True(currentTerm + 1 < maxTerm, $"Followers didn't become leaders although old leader can't communicate with the cluster in term {currentTerm} (max term: {maxTerm})");
                    Assert.True(maxTerm < 10, "Followers were unable to elect a leader.");
                    currentTerm = maxTerm;
                    waitingList.Clear();
                }
            }

            Assert.True(await firstLeader.WaitForLeaveState(RachisState.Leader,CancellationToken.None).WaitAsync(timeToWait));

            var newLeaderLastIndex = await IssueCommandsAndWaitForCommit(5, "test", 1);
            if (Log.IsInfoEnabled)
            {
                Log.Info("Reconnect old leader");
            }
            ReconnectToNode(firstLeader);

            var retries = numberOfNodes;
            do
            {
                var waitForCommitIndexChange = firstLeader.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newLeaderLastIndex);
                if (await waitForCommitIndexChange.WaitAsync(timeToWait))
                {
                    break;
                }
            } while (retries-- > 0);

            Assert.True(retries > 0,
                $"Old leader is in {firstLeader.CurrentState} state and didn't rollback his log to the new leader log (last index: {GetLastCommittedIndex(firstLeader)}, expected: {newLeaderLastIndex})");
            Assert.Equal(numberOfNodes, RachisConsensuses.Count);

            foreach (var invalidCommand in invalidCommands)
            {
                await Assert.ThrowsAsync<NotLeadingException>(() => invalidCommand);

                Assert.True(invalidCommand.IsCompleted);
                Assert.NotEqual(TaskStatus.RanToCompletion, invalidCommand.Status);
            }
        }

        [Fact]
        public async Task RavenDB_13228()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var numberOfNodes = 2;
            var firstLeader = await CreateNetworkAndGetLeader(numberOfNodes);
            var follower = GetFollowers().Single();

            var timeToWait = TimeSpan.FromMilliseconds(1000 * numberOfNodes);
            await IssueCommandsAndWaitForCommit(10, "test", 1);
            var currentTerm = firstLeader.CurrentTerm;

            var t = Task.Run(() => IssueCommandsWithoutWaitingForCommits(firstLeader, 100, "test"));
            Disconnect(follower.Url, firstLeader.Url);
            await t;

            Assert.True(await firstLeader.WaitForState(RachisState.Candidate, CancellationToken.None).WaitAsync(timeToWait),$"{firstLeader.CurrentState}");
            follower.FoundAboutHigherTerm(currentTerm + 1," why not, should work!");
            Reconnect(follower.Url, firstLeader.Url);


            Assert.True(await firstLeader.WaitForState(RachisState.Leader, CancellationToken.None).WaitAsync(timeToWait),
                $"leader: {firstLeader.CurrentState} in term {firstLeader.CurrentTerm} with last index {GetLastCommittedIndex(firstLeader)}{Environment.NewLine}, " +
                $"follower: state {follower.CurrentState} in term {follower.CurrentTerm} with last index {GetLastCommittedIndex(follower)}");
            Assert.Equal(currentTerm + 2, firstLeader.CurrentTerm);


            var count = 100;
            while (true)
            {
                count--;
                var last = GetLastAppendedIndex(firstLeader);
                var index = GetLastCommittedIndex(follower);

                if (last == index)
                    break;

                await Task.Delay(TimeSpan.FromMilliseconds(100));
                if (count == 0)
                    Assert.False(true, $"last appended index in the leader is {last}, last committed index in the follower is {index}");
            }

            Assert.Equal(GetLastAppendedIndex(firstLeader), GetLastAppendedIndex(follower));

            using (firstLeader.ContextPool.AllocateOperationContext(out TransactionOperationContext leaderContext))
            using (follower.ContextPool.AllocateOperationContext(out TransactionOperationContext followerContext))
            using (leaderContext.OpenReadTransaction())
            using (followerContext.OpenReadTransaction())
            {
                var leaderValue = firstLeader.StateMachine.Read(leaderContext, "test");
                var followerValue = follower.StateMachine.Read(followerContext, "test");
                Assert.Equal(leaderValue, followerValue);
            }
        }

        private long GetLastCommittedIndex(RachisConsensus<CountingStateMachine> rachis)
        {
            using (rachis.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return rachis.GetLastCommitIndex(context);
            }
        }

        private long GetLastAppendedIndex(RachisConsensus<CountingStateMachine> rachis)
        {
            using (rachis.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return rachis.GetLastEntryIndex(context);
            }
        }
    }
}
