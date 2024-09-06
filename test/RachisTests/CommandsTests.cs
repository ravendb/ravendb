using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    public class CommandsTests : RachisConsensusTestBase
    {
        public CommandsTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Command_sent_twice_should_not_throw_timeout_error()
        {
            var leader = await CreateNetworkAndGetLeader(3);
            var nonLeader = GetRandomFollower();
            long lastIndex;
            using (leader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                var guid = RaftIdGenerator.NewId();
                var cmd = new TestCommandWithRaftId("test", guid)
                {
                    RaftCommandIndex = 322
                };

                var cloned = new TestCommandWithRaftId("test", guid)
                {
                    RaftCommandIndex = 322
                };

                var t = leader.SendToLeaderAsync(cmd);
                await leader.SendToLeaderAsync(cloned);

                // this should not throw timeout exception.
                var exception = await Record.ExceptionAsync(async () => await t);
                Assert.Null(exception);

                using (context.OpenReadTransaction())
                    lastIndex = leader.GetLastEntryIndex(context);
            }
            var waitForAllCommits = nonLeader.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex);
            Assert.True(await waitForAllCommits.WaitWithoutExceptionAsync(LongWaitTime), "didn't commit in time");
        }

        [Fact]
        public async Task When_command_committed_CompletionTaskSource_is_notified()
        {
            const int commandCount = 10;
            const int clusterSize = 3;
            var leader = await CreateNetworkAndGetLeader(clusterSize);
            var nonLeader = GetRandomFollower();
            var tasks = new List<Task>();
            long lastIndex;

            using (leader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                for (var i = 0; i < commandCount; i++)
                {
                    tasks.Add(leader.SendToLeaderAsync(new TestCommand { Name = "test", Value = i }));
                }
                using (context.OpenReadTransaction())
                    lastIndex = leader.GetLastEntryIndex(context);
            }
            var waitForAllCommits = nonLeader.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex);

            Assert.True(await Task.WhenAny(waitForAllCommits, Task.Delay(LongWaitTime)) == waitForAllCommits, "didn't commit in time");
            var waitForNotificationsOnTasks = Task.WhenAll(tasks);
            Assert.True(await Task.WhenAny(waitForNotificationsOnTasks, Task.Delay(LongWaitTime)) == waitForNotificationsOnTasks, "Some commands didn't complete");
        }

        [Fact]
        public async Task Command_not_committed_after_timeout_CompletionTaskSource_is_notified()
        {
            const int commandCount = 3;
            const int clusterSize = 3;
            var leader = await CreateNetworkAndGetLeader(clusterSize);
            var nonLeader = GetRandomFollower();
            var tasks = new List<Task>();
            long lastIndex;

            using (leader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            {
                for (var i = 0; i < commandCount; i++)
                {
                    tasks.Add(leader.SendToLeaderAsync(new TestCommand { Name = "test", Value = i }));
                }
                using (context.OpenReadTransaction())
                    lastIndex = leader.GetLastEntryIndex(context);
            }
            var waitForAllCommits = nonLeader.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex);
            Assert.True(await waitForAllCommits.WaitWithoutExceptionAsync(LongWaitTime), "didn't commit in time");

            Assert.True(await Task.WhenAll(tasks).WaitWithoutExceptionAsync(TimeSpan.FromSeconds(15)), $"Some commands didn't complete");
            DisconnectFromNode(leader);
            
            try
            {
                var task = leader.CurrentLeader.PutAsync(new TestCommand { Name = "test", Value = commandCount },
                    TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 10));
                await task;
                Assert.Fail("We should have gotten an error");
            }
            // expecting either one of those
            catch (TimeoutException)
            {
            }
            catch (NotLeadingException)
            {
            }
        }
    }
}
