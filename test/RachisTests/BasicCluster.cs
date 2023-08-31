using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    public class BasicCluster : RachisConsensusTestBase
    {
        public BasicCluster(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task PreventConcurrentBootstrap()
        {
            var a = SetupServer();
            var tasks = new List<Task>();
            for (int i = 0; i < 20; i++)
            {
                tasks.Add(Task.Run(() => a.Bootstrap(a.Url, "A")));
            }

            await Task.WhenAll(tasks);
            foreach (var task in tasks)
            {
                await task;
            }

            using (a.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var actual = a.GetLastEntryIndex(context);
                Assert.Equal(1, actual);
            }
        }

        [Fact]
        public async Task ClusterWithFiveNodesAndMultipleElections()
        {
            PredictableSeeds = true;
            var a = SetupServer(true);
            var b = SetupServer();
            var c = SetupServer();
            var d = SetupServer();
            var e = SetupServer();

            var followers = new[] { b, c, d, e };
            foreach (var follower in followers)
            {
                await a.AddToClusterAsync(follower.Url);
                await follower.WaitForTopology(Leader.TopologyModification.Voter);
            }

            var leaderSelected = followers.Select(x => x.WaitForState(RachisState.Leader, CancellationToken.None).ContinueWith(_ => x)).ToArray();
            for (int i = 0; i < 10; i++)
            {
                await a.PutAsync(new TestCommand { Name = "test", Value = i });
            }
            foreach (var follower in followers)
            {
                Disconnect(follower.Url, a.Url);
            }

            var leader = await await Task.WhenAny(leaderSelected);
            for (int i = 10; i < 20; i++)
            {
                await leader.PutAsync(new TestCommand { Name = "test", Value = i });
            }

            followers = followers.Except(new[] { leader }).ToArray();
            leaderSelected = followers.Select(x => x.WaitForState(RachisState.Leader, CancellationToken.None).ContinueWith(_ => x)).ToArray();

            foreach (var follower in followers)
            {
                Disconnect(follower.Url, leader.Url);
            }

            leader = await await Task.WhenAny(leaderSelected);
            for (int i = 20; i < 30; i++)
            {
                await leader.PutAsync(new TestCommand { Name = "test", Value = i });
            }

            using (leader.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                var actual = leader.StateMachine.Read(context, "test");
                var expected = string.Join("", Enumerable.Range(0, 30));
                Assert.Equal(expected, actual);
            }
        }

        [Fact]
        public async Task ClusterWithThreeNodesAndElections()
        {
            var a = SetupServer(true);
            var b = SetupServer();
            var c = SetupServer();

            var bUpgraded = b.WaitForTopology(Leader.TopologyModification.Voter);
            var cUpgraded = c.WaitForTopology(Leader.TopologyModification.Voter);

            await a.AddToClusterAsync(b.Url);
            await b.WaitForTopology(Leader.TopologyModification.Voter);
            await a.AddToClusterAsync(c.Url);
            await c.WaitForTopology(Leader.TopologyModification.Voter);

            await bUpgraded;
            await cUpgraded;

            var bLeader = b.WaitForState(RachisState.Leader, CancellationToken.None);
            var cLeader = c.WaitForState(RachisState.Leader, CancellationToken.None);

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                for (int i = 0; i < 10; i++)
                {
                    await a.PutAsync(new TestCommand { Name = "test", Value = i });
                }
            }

            Disconnect(b.Url, a.Url);
            Disconnect(c.Url, a.Url);

            await Task.WhenAny(bLeader, cLeader);
        }

        [Fact]
        public async Task ClusterWithLateJoiningNodeRequiringSnapshot()
        {
            var expected = "0123456789";
            var a = SetupServer(true);

            for (var i = 0; i < 5; i++)
            {
                await a.PutAsync(new TestCommand { Name = "test", Value = i });
            }

            var b = SetupServer();
            await a.AddToClusterAsync(b.Url);
            await b.WaitForTopology(Leader.TopologyModification.Voter);
            long lastIndex = 0;
            for (var i = 0; i < 5; i++)
            {
                var (etag, _) = await a.PutAsync(new TestCommand { Name = "test", Value = i + 5 });
                lastIndex = etag;
            }

            Assert.True(await b.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex).WaitWithoutExceptionAsync(15000));
            using (b.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                Assert.Equal(expected, b.StateMachine.Read(context, "test"));
            }
        }

        [Fact]
        public async Task ClusterWithTwoNodes()
        {
            var expected = "0123456789";
            var a = SetupServer(true);
            var b = SetupServer();

            await a.AddToClusterAsync(b.Url);
            await b.WaitForTopology(Leader.TopologyModification.Voter);

            var tasks = new List<Task>();
            for (var i = 0; i < 9; i++)
            {
                tasks.Add(a.PutAsync(new TestCommand { Name = "test", Value = i }));
            }

            var (lastIndex, _) = await a.PutAsync(new TestCommand { Name = "test", Value = 9 });
            var waitForCommitIndexChange = b.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, lastIndex);
            Assert.True(await waitForCommitIndexChange.WaitWithoutExceptionAsync(TimeSpan.FromSeconds(5)));

            using (b.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                Assert.Equal(expected, b.StateMachine.Read(context, "test"));
            }
        }

        [Fact]
        public async Task CanSetupSingleNode()
        {
            var rachis = SetupServer(true);

            for (var i = 0; i < 10; i++)
            {
                await rachis.PutAsync(new TestCommand { Name = "test", Value = i });
            }

            using (rachis.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
            using (context.OpenReadTransaction())
            {
                Assert.Equal("0123456789", rachis.StateMachine.Read(context, "test"));
            }
        }
    }
}
