using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    public class TopologyChangesTests : RachisConsensusTestBase
    {
        public TopologyChangesTests(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task CanEnforceTopologyOnOldLeader()
        {
            var leader = await CreateNetworkAndGetLeader(3);
            var followers = GetFollowers();
            var newServer = SetupServer();
            DisconnectFromNode(leader);
            await Assert.ThrowsAnyAsync<Exception>(() => leader.AddToClusterAsync(newServer.Url));
            var newLeader = WaitForAnyToBecomeLeader(followers);

            Assert.NotNull(newLeader);
            ReconnectToNode(leader);

            Assert.True(await leader.WaitForTopology(Leader.TopologyModification.Remove, newServer.Url).WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 6))); // was 'TotalMilliseconds * 3', changed to *6 for low end machines RavenDB-7263
        }
        /// <summary>
        /// This test checks that a node could be added to the cluster even if the node is down.
        /// We mimic a node been down by giving a url that doesn't exists.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task New_node_can_be_added_even_if_it_is_down()
        {
            var leader = await CreateNetworkAndGetLeader(3);
            Assert.True(await leader.AddToClusterAsync("http://rachis.example.com:1337").WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 2)), "non existing node should be able to join the cluster");
            List<Task> waitingList = new List<Task>();
            foreach (var consensus in RachisConsensuses)
            {
                waitingList.Add(consensus.WaitForTopology(Leader.TopologyModification.Promotable, "D"));
            }
            Assert.True(await Task.WhenAll(waitingList).WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 2)), "Cluster was non informed about new node within two election periods");
        }

        /// <summary>
        /// This test creates two nodes that don't exists and then setup those two nodes and make sure they are been updated with the current log.
        /// </summary>
        [Fact]
        public async Task Adding_additional_node_that_goes_offline_and_then_online_should_still_work()
        {
            var node4 = SetupServer();
            var node5 = SetupServer();
            DisconnectFromNode(node4);
            DisconnectFromNode(node5);
            var leader = await CreateNetworkAndGetLeader(3);
            Assert.True(await leader.AddToClusterAsync(node4.Url).WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 2)), "non existing node should be able to join the cluster");
            Assert.True(await leader.AddToClusterAsync(node5.Url).WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 2)), "non existing node should be able to join the cluster");
            var t = IssueCommandsAndWaitForCommit(3, "test", 1);
            Assert.True(await t.WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 2)), "Commands were not committed in time although there is a majority of active nodes in the cluster");

            ReconnectToNode(node4);
            ReconnectToNode(node5);

            Assert.True(await node4.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, await t).WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 2)),
                "#D server didn't get the commands in time");
            Assert.True(await node5.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, await t).WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 2)),
                "#E server didn't get the commands in time");
        }

        [Fact]
        public async Task Adding_already_existing_node_should_throw()
        {
            var leader = await CreateNetworkAndGetLeader(3);
            Assert.True(await leader.AddToClusterAsync("http://not-a-real-url.com").WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 2)),
                "non existing node should be able to join the cluster");
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => leader.AddToClusterAsync("http://not-a-real-url.com"));
        }


        [Fact]
        public async Task Removal_of_non_existing_node_should_throw()
        {
            var leader = await CreateNetworkAndGetLeader(3);
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => leader.RemoveFromClusterAsync("ABCD"));
        }

        [Theory]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task Non_leader_Node_removed_from_cluster_should_update_peers_list(int nodeCount)
        {
            var leader = await CreateNetworkAndGetLeader(nodeCount);
            var follower = GetRandomFollower();
            Assert.True(await leader.RemoveFromClusterAsync(follower.Tag).WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 10)), "Was unable to remove node from cluster in time");
            foreach (var node in RachisConsensuses)
            {
                if (node.Url == follower.Url)
                    continue;
                Assert.True(await node.WaitForTopology(Leader.TopologyModification.Remove, follower.Tag).WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(node.ElectionTimeout.TotalMilliseconds * 1000)), "Node was not removed from topology in time");
            }
        }

        [Fact]
        public async Task AddingRemovedNodeShouldWork()
        {
            var clusterSize = 3;
            var leader = await CreateNetworkAndGetLeader(clusterSize);
            var follower = GetRandomFollower();

            var oldTag = follower.Tag;
            var url = follower.Url;
            Assert.True(await leader.RemoveFromClusterAsync(oldTag).WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 10)), "Was unable to remove node from cluster in time");
            foreach (var node in RachisConsensuses)
            {
                if (node.Url == url)
                    continue;
                Assert.True(await node.WaitForTopology(Leader.TopologyModification.Remove, follower.Tag).WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(node.ElectionTimeout.TotalMilliseconds * 10)), "Node was not removed from topology in time");
            }

            follower.Url = url;
            var isAddedSuccessfully = await leader.AddToClusterAsync(follower.Url, follower.Tag).WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 5));
            Assert.True(isAddedSuccessfully);
            var waitForTopologySuccessful = await follower.WaitForTopology(Leader.TopologyModification.Voter).WaitWithoutExceptionAsync(TimeSpan.FromMilliseconds(leader.ElectionTimeout.TotalMilliseconds * 5));
            Assert.True(waitForTopologySuccessful);

            using (leader.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var topology = leader.GetTopology(ctx);
                Assert.Equal(clusterSize, topology.AllNodes.Count);
                Assert.True(topology.Contains(oldTag));
            }
        }


        [Fact]
        public async Task AddNodeFromDifferentCluster()
        {
            var node1 = await CreateNetworkAndGetLeader(1);
            var node2 = await CreateNetworkAndGetLeader(1);
            var url = node2.Url;

            await node1.SendToLeaderAsync(new TestCommand { Name = "test", Value = 10 });
            await node2.SendToLeaderAsync(new TestCommand { Name = "test", Value = 20 });

            string id;
            using (node1.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                id = node1.GetTopology(ctx).TopologyId;
            }

            await node2.HardResetToPassiveAsync(id);
            using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
            {
                await node2.WaitForState(RachisState.Passive, cts.Token);
            }

            await node1.AddToClusterAsync(url);
            await node2.WaitForTopology(Leader.TopologyModification.Voter);

            using (node1.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx1))
            using (node2.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx2))
            using (ctx1.OpenReadTransaction())
            using (ctx2.OpenReadTransaction())
            {
                var val1 = node1.StateMachine.Read(ctx1, "test");
                var val2 = node1.StateMachine.Read(ctx2, "test");
                Assert.Equal(10.ToString(), val1);
                Assert.Equal(10.ToString(), val2);
            }
        }
    }
}
