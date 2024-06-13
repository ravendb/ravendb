using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    public class ElectionTests : RachisConsensusTestBase
    {
        public ElectionTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Follower_as_a_single_node_becomes_leader_automatically()
        {
            var node = SetupServer(true);
            var nodeCurrentState = node.CurrentState;
            Assert.True(nodeCurrentState == RachisState.LeaderElect ||
                        nodeCurrentState == RachisState.Leader);
            var waitForState = node.WaitForState(RachisState.Leader, CancellationToken.None);

            var condition = await waitForState.WaitWithoutExceptionAsync(10 * node.ElectionTimeout);
            Assert.True(condition, $"Node is in state {node.CurrentState} and didn't become leader although he is alone in his cluster.");
        }


        [Fact]
        public async Task RavenDB_13922()
        {
            DebuggerAttachedTimeout.DisableLongTimespan = true;
            var leader = await CreateNetworkAndGetLeader(2);
            var follower = GetFollowers().Single();
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            
            DisconnectBiDirectionalFromNode(leader);

            using (leader.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction())
            {
                leader.SetNewStateInTx(ctx, RachisState.LeaderElect, null, leader.CurrentTerm, "append an extra entry so only me can be leader");
                tx.Commit();
            }
            
            var mre1 = new ManualResetEventSlim(false);
            var mre2 = new ManualResetEventSlim(false);

            leader.ForTestingPurposesOnly().CreateHoldOnLeaderElect(mre1);
            follower.ForTestingPurposesOnly().CreateHoldOnLeaderElect(mre2);

            await leader.WaitForState(RachisState.Candidate, cts.Token);
            await follower.WaitForState(RachisState.Candidate, cts.Token);

            var le1 = leader.WaitForState(RachisState.LeaderElect, cts.Token);
            var le2 = follower.WaitForState(RachisState.LeaderElect, cts.Token);

            ReconnectBiDirectionalFromNode(leader);
            
            while (le1.IsCompleted == false && le2.IsCompleted == false)
            {
                mre1.Set();
                mre2.Set();
                await Task.Delay(100, cts.Token);
            }

            await Task.WhenAny(le1, le2);

            await leader.WaitForState(RachisState.Candidate, cts.Token);
            await follower.WaitForState(RachisState.Candidate, cts.Token);

            leader.ForTestingPurposesOnly().HoldOnLeaderElect = null;
            mre1.Set();

            follower.ForTestingPurposesOnly().HoldOnLeaderElect = null;
            mre2.Set();

            var lastIndex = await IssueCommandsAndWaitForCommit(3, "test", 1);

            var t1 = leader.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex, token: cts.Token);
            var t2 = follower.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex, token: cts.Token);
            if (await Task.WhenAll(t1, t2).WaitWithoutExceptionAsync(5000) == false)
            {
                throw new TimeoutException();
            }
        }

        [Fact]
        public async Task CanElectOnDivergence4()
        {
            var firstLeader = await CreateNetworkAndGetLeader(3);
            var followers = GetFollowers();

            var follower1 = followers[0];
            var follower2 = followers[1];

            DisconnectBiDirectionalFromNode(follower1);

            var lastIndex = -1L;
            var leaderTerm = firstLeader.CurrentTerm;
            await IssueCommandsAndWaitForCommit(3, "foo", 123);

            DisconnectBiDirectionalFromNode(firstLeader);

            for (int i = 0; i < 6; i++)
            {
                AppendToLog(firstLeader, new TestCommand { Name = "bar", Value = 1 }, leaderTerm);
            }
            await firstLeader.WaitForState(RachisState.Candidate, CancellationToken.None);

            long truncatedIndex;
            using (firstLeader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                RachisConsensus.GetLastTruncated(context, out truncatedIndex, out var truncatedTerm);
            }

            Reconnect(follower1.Url, follower2.Url);
            Reconnect(follower2.Url, follower1.Url);
            var newLeader = WaitForAnyToBecomeLeader(followers);

            await IssueCommandsAndWaitForCommit(20, "baz", 123);

            var nonLeader = followers.Single(x => x != newLeader);
            DisconnectBiDirectionalFromNode(nonLeader);

            using (newLeader.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction())
            {
                newLeader.TruncateLogBefore(ctx, truncatedIndex + 3);
                tx.Commit();
            }

            Reconnect(firstLeader.Url, newLeader.Url);
            Reconnect(newLeader.Url, firstLeader.Url);

            using (newLeader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                lastIndex = newLeader.GetLastCommitIndex(context);
            }

            var condition = await firstLeader.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex)
                    .WaitWithoutExceptionAsync(5000);

            using (firstLeader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var commitIndex = firstLeader.GetLastCommitIndex(context);
                Assert.True(condition, $"Last commit is {commitIndex} wanted {lastIndex}");
            }

            ReconnectBiDirectionalFromNode(nonLeader);

            lastIndex = await IssueCommandsAndWaitForCommit(10, "foo", 357);

            foreach (var r in RachisConsensuses)
            {
                condition = await r.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex)
                    .WaitWithoutExceptionAsync(5000);

                using (r.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var commitIndex = r.GetLastCommitIndex(context);
                    Assert.True(condition, $"Last commit is {commitIndex} wanted {lastIndex}");
                }
            }
        }

        [Fact]
        public async Task CanElectOnDivergence3()
        {
            var firstLeader = await CreateNetworkAndGetLeader(3);
            var followers = GetFollowers();

            var randFollower = followers.First();
            DisconnectBiDirectionalFromNode(randFollower);

            var leaderTerm = firstLeader.CurrentTerm;
            await IssueCommandsAndWaitForCommit(10, "foo", 123);

            ReconnectBiDirectionalFromNode(randFollower);
            DisconnectBiDirectionalFromNode(firstLeader);

            for (int i = 0; i < 10; i++)
            {
                AppendToLog(firstLeader, new TestCommand { Name = "bar", Value = 1 }, leaderTerm);
            }

            await firstLeader.WaitForState(RachisState.Candidate, CancellationToken.None);

            var newLeader = WaitForAnyToBecomeLeader(followers);

            var lastIndex = -1L;

            using (firstLeader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                lastIndex = firstLeader.GetLastCommitIndex(context);
            }

            await IssueCommandsAndWaitForCommit(10, "baz", 123);
            var nonLeader = followers.Single(x => x != newLeader);
            DisconnectBiDirectionalFromNode(nonLeader);

            using (newLeader.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction())
            {
                newLeader.TruncateLogBefore(ctx, lastIndex);
                tx.Commit();
            }

            ReconnectBiDirectionalFromNode(firstLeader);

            using (newLeader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                lastIndex = newLeader.GetLastCommitIndex(context);
            }

            var condition = await firstLeader.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex)
                    .WaitWithoutExceptionAsync(5000);

            using (firstLeader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var commitIndex = firstLeader.GetLastCommitIndex(context);
                Assert.True(condition, $"Last commit is {commitIndex} wanted {lastIndex}");
            }

            ReconnectBiDirectionalFromNode(nonLeader);

            lastIndex = await IssueCommandsAndWaitForCommit(10, "foo", 357);

            foreach (var r in RachisConsensuses)
            {
                condition = await r.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex)
                        .WaitWithoutExceptionAsync(5000);

                using (r.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var commitIndex = r.GetLastCommitIndex(context);
                    Assert.True(condition, $"Last commit is {commitIndex} wanted {lastIndex}");
                }
            }
        }

        [Fact]
        public async Task CanElectOnDivergence2()
        {
            var firstLeader = await CreateNetworkAndGetLeader(3);
            var flag = new MultipleUseFlag();
            foreach (var follower in RachisConsensuses)
            {
                follower.ForTestingPurposesOnly().CreateLeaderLock(flag);
            }

            firstLeader.CurrentLeader.StepDown(forceElection: false);
            await Task.Delay(1000);

            WaitForAnyToBecomeLeader(RachisConsensuses);
            var lastIndex = await IssueCommandsAndWaitForCommit(30, "test", 1);

            foreach (var node in RachisConsensuses)
            {
                node.ForTestingPurposes?.LeaderLock?.Awake();
            }

            foreach (var r in RachisConsensuses)
            {
                var condition = await
                    r.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex)
                        .WaitWithoutExceptionAsync(10000);

                using (r.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var commitIndex = r.GetLastCommitIndex(context);
                    Assert.True(condition, $"Last commit is {commitIndex} wanted {lastIndex}");
                }
            }
        }

        [Fact]
        public async Task CanElectOnDivergence()
        {
            var firstLeader = await CreateNetworkAndGetLeader(3);
            var followers = GetFollowers();

            var timeToWait = TimeSpan.FromMilliseconds(3000);
            await IssueCommandsAndWaitForCommit(3, "test", 1);
            var currentTerm = firstLeader.CurrentTerm;
            DisconnectBiDirectionalFromNode(firstLeader);


            using (firstLeader.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (var tx = ctx.OpenWriteTransaction())
            {
                var cmd = new TestCommand
                {
                    Name = "bar",
                    Value = 1,
                    UniqueRequestId = Guid.NewGuid().ToString()
                };


                firstLeader.InsertToLeaderLog(ctx, currentTerm, ctx.ReadObject(cmd.ToJson(ctx), "bar"), RachisEntryFlags.StateMachineCommand);
                tx.Commit();
            }
            Assert.True(await firstLeader.WaitForLeaveState(RachisState.Leader, CancellationToken.None).WaitWithoutExceptionAsync(timeToWait));

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
                    var done = await Task.WhenAny(waitingList).WaitWithoutExceptionAsync(timeToWait);
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


            var newLeaderLastIndex = await IssueCommandsAndWaitForCommit(5, "test", 1);
            if (Log.IsInfoEnabled)
            {
                Log.Info("Reconnect old leader");
            }
            ReconnectBiDirectionalFromNode(firstLeader);
            var retries = 3;
            do
            {
                var waitForCommitIndexChange = firstLeader.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, newLeaderLastIndex);
                if (await waitForCommitIndexChange.WaitWithoutExceptionAsync(timeToWait))
                {
                    break;
                }
            } while (retries-- > 0);

            Assert.True(retries > 0,
                $"Old leader is in {firstLeader.CurrentState} state and didn't rollback his log to the new leader log (last index: {GetLastCommittedIndex(firstLeader)}, expected: {newLeaderLastIndex})");
            Assert.Equal(3, RachisConsensuses.Count);
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(5)]
        public async Task ForceStepDownAndElectNewLeader(int numberOfNodes)
        {
            var firstLeader = await CreateNetworkAndGetLeader(numberOfNodes);
            firstLeader.CurrentLeader.StepDown();
            Assert.True(await firstLeader.WaitForState(RachisState.Follower, CancellationToken.None).WaitWithoutExceptionAsync(TimeSpan.FromSeconds(30)), $"Old leader hasn't stepped down, firstLeader.CurrentState={firstLeader.CurrentState}.");
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
                    var done = await Task.WhenAny(waitingList).WaitWithoutExceptionAsync(timeToWait);
                    if (done)
                    {
                        break;
                    }

                    var maxTerm = followers.Max(f => f.CurrentTerm);
                    RavenTestHelper.AssertTrue(currentTerm + 1 <= maxTerm, () =>
                        $"Followers didn't become leaders although old leader can't communicate with the cluster in term {currentTerm} (max term: {maxTerm})" +
                        $"{string.Join(',', followers.Select(f => $"{f.Tag}={f.CurrentState}:{f.CurrentTerm}"))}");

                    Assert.True(maxTerm < 10, "Followers were unable to elect a leader.");
                    currentTerm = maxTerm;
                    waitingList.Clear();
                }
            }

            Assert.True(await firstLeader.WaitForLeaveState(RachisState.Leader, CancellationToken.None).WaitWithoutExceptionAsync(timeToWait));

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
                if (await waitForCommitIndexChange.WaitWithoutExceptionAsync(timeToWait))
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

            var timeToWait = TimeSpan.FromMilliseconds(5000 * numberOfNodes);
            await IssueCommandsAndWaitForCommit(10, "test", 1);
            var currentTerm = firstLeader.CurrentTerm;

            Disconnect(follower.Url, firstLeader.Url);

            // append to previous leader
            AppendToLog(firstLeader, new TestCommand { Name = "foo", Value = 123 }, currentTerm);

            Assert.True(await firstLeader.WaitForState(RachisState.Candidate, CancellationToken.None).WaitWithoutExceptionAsync(timeToWait), $"{firstLeader.CurrentState}");
            follower.FoundAboutHigherTerm(currentTerm + 1, " why not, should work!");
            Reconnect(follower.Url, firstLeader.Url);

            RavenTestHelper.AssertTrue(await firstLeader.WaitForState(RachisState.Leader, CancellationToken.None).WaitWithoutExceptionAsync(timeToWait), () =>
                $"leader: {firstLeader.CurrentState} in term {firstLeader.CurrentTerm} with last index {GetLastCommittedIndex(firstLeader)}{Environment.NewLine}, " +
                $"follower: state {follower.CurrentState} in term {follower.CurrentTerm} with last index {GetLastCommittedIndex(follower)}{Environment.NewLine}" +
                $"{GetCandidateStatus(RachisConsensuses)}");

            Assert.True(currentTerm + 2 <= firstLeader.CurrentTerm, $"{currentTerm} + 2 <= {firstLeader.CurrentTerm}");

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
                    Assert.Fail($"last appended index in the leader is {last}, last committed index in the follower is {index}");
            }

            Assert.Equal(GetLastAppendedIndex(firstLeader), GetLastAppendedIndex(follower));

            using (firstLeader.ContextPool.AllocateOperationContext(out ClusterOperationContext leaderContext))
            using (follower.ContextPool.AllocateOperationContext(out ClusterOperationContext followerContext))
            using (leaderContext.OpenReadTransaction())
            using (followerContext.OpenReadTransaction())
            {
                var leaderValue = firstLeader.StateMachine.Read(leaderContext, "test");
                var followerValue = follower.StateMachine.Read(followerContext, "test");
                Assert.Equal(leaderValue, followerValue);
                Assert.Equal(new string('1', 10), followerValue);
            }
        }



        private long GetLastCommittedIndex(RachisConsensus<CountingStateMachine> rachis)
        {
            using (rachis.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                return rachis.GetLastCommitIndex(context);
            }
        }

        private long GetLastAppendedIndex(RachisConsensus<CountingStateMachine> rachis)
        {
            using (rachis.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                return rachis.GetLastEntryIndex(context);
            }
        }

        public long AppendToLog(RachisConsensus<CountingStateMachine> engine, CommandBase cmd, long term)
        {
            // using (engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            // {
            //     var djv = cmd.ToJson(context);
            //     var cmdJson = context.ReadObject(djv, "raft/command");
            //
            //     using (var tx = context.OpenWriteTransaction())
            //     {
            //         var index = engine.InsertToLeaderLog(context, term, cmdJson, RachisEntryFlags.StateMachineCommand);
            //         tx.Commit();
            //
            //         return index;
            //     }
            // }

            var index = -1L;

            engine.TxMerger.EnqueueSync((context) =>
            {

                var djv = cmd.ToJson(context);
                var cmdJson = context.ReadObject(djv, "raft/command");


                index = engine.InsertToLeaderLog(context, term, cmdJson, RachisEntryFlags.StateMachineCommand);

                return 1;
            });

            return index;
        }
    }
}
