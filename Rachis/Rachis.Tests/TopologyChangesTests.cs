using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using FizzWare.NBuilder;
using FluentAssertions;
using Rachis.Transport;
using Xunit;
using Xunit.Extensions;

namespace Rachis.Tests
{
    public class TopologyChangesTests : RaftTestsBase
    {
        private static int testCount = 0;

        public TopologyChangesTests ()
        {
            WriteLine ("------------------------------------------------");
            WriteLine ("--== Starting test " + (testCount++) + " ==--");
            WriteLine ("------------------------------------------------");
        }

        [Fact]
        public void CanRevertTopologyChange()
        {
            var leader = CreateNetworkAndGetLeader(3);
            var nonLeaders = Nodes.Where(x => x != leader).ToList();
            var inMemoryTransport = ((InMemoryTransportHub.InMemoryTransport) leader.Transport);

            DisconnectNodeSending(leader.Name);

            WriteLine("Initial leader is " + leader.Name);
            leader.AddToClusterAsync(new NodeConnectionInfo { Name = "node3" });

            var topologyChanged = WaitForToplogyChange(leader);

            Assert.True(leader.CurrentTopology.Contains("node2"));

            WriteLine("<-- should switch leaders now");

            nonLeaders.ForEach(engine => inMemoryTransport.ForceTimeout());

            inMemoryTransport.ForceTimeout();// force it to win

            Assert.True(topologyChanged.Wait(3000));

            Assert.True(nonLeaders.Any(x=>x.State==RaftEngineState.Leader));
            foreach (var raftEngine in nonLeaders)
            {
                Assert.False(raftEngine.CurrentTopology.IsVoter("node3"));
            }
            Assert.False(leader.CurrentTopology.IsVoter("node3"));

        }

        //fire up console to listen for log : nc -l -u -p 7071
        [Fact]
        public void New_node_can_be_added_even_if_it_is_down()
        {
            const int nodeCount = 3;

            var topologyChangeFinishedOnAllNodes = new CountdownEvent(nodeCount);
            var leader = CreateNetworkAndGetLeader(nodeCount);

            Nodes.ToList().ForEach(n => n.TopologyChanged += cmd => {
                if(cmd.Requested.AllNodeNames.Contains("non-existing-node"))
                    topologyChangeFinishedOnAllNodes.Signal(); 
            });

            // ReSharper disable once PossibleNullReferenceException
            leader.AddToClusterAsync(new NodeConnectionInfo { Name = "non-existing-node" }).Wait();

            Assert.True(topologyChangeFinishedOnAllNodes.Wait(5000),"Topology changes should happen in less than 5 sec for 3 node network");
            Nodes.ToList().ForEach(n => 
            {
                    var allNodeNames = n.CurrentTopology.AllNodeNames.ToList();
                    allNodeNames.Should().Contain("non-existing-node");
            });
        }


        [Fact]
        public void Adding_additional_node_that_goes_offline_and_then_online_should_still_work()
        {
            var leaderNode = CreateNetworkAndGetLeader(3);

            using (var additionalNode = NewNodeFor(leaderNode))
            {
                additionalNode.TopologyChanging += () => DisconnectNode("node3");
                var waitForTopologyChangeInLeader = WaitForToplogyChange(leaderNode);

                leaderNode.AddToClusterAsync(new NodeConnectionInfo { Name = additionalNode.Name }).Wait();

                Thread.Sleep(additionalNode.Options.ElectionTimeout * 2);
                ReconnectNode(additionalNode.Name);

                Assert.True(waitForTopologyChangeInLeader.Wait(3000));
            }
        }

        [Fact]
        public void Adding_already_existing_node_should_throw()
        {
            var leader = CreateNetworkAndGetLeader(2);
            leader.Invoking(x => x.AddToClusterAsync(new NodeConnectionInfo
            {
                Name = Nodes.First(a => a.Name != leader.Name).Name
            }))
                .ShouldThrow<InvalidOperationException>();
        }

        [Fact]
        public void Removal_of_non_existing_node_should_throw()
        {
            var leader = CreateNetworkAndGetLeader(2);
            leader.Invoking(x => x.RemoveFromClusterAsync(new NodeConnectionInfo { Name = "santa" }))
                .ShouldThrow<InvalidOperationException>();

        }

        [Fact]
        public void Cluster_cannot_have_two_concurrent_node_removals()
        {
            var leader = CreateNetworkAndGetLeader(4, messageTimeout: 1500);

            var nonLeader = Nodes.FirstOrDefault(x => x.State != RaftEngineState.Leader);
            Assert.NotNull(nonLeader);

            leader.RemoveFromClusterAsync(new NodeConnectionInfo { Name = nonLeader.Name });

            //if another removal from cluster is in progress, 
            Assert.Throws<InvalidOperationException>(() => leader.RemoveFromClusterAsync(new NodeConnectionInfo { Name = leader.Name }).Wait());
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public void Non_leader_Node_removed_from_cluster_should_update_peers_list(int nodeCount)
        {
            var leader = CreateNetworkAndGetLeader(nodeCount);

            var cmd = new DictionaryCommand.Set
            {
                Key = "a",
                Value = 1
            };

            var waitForCommitsOnCluster = WaitForCommitsOnCluster(machine => machine.LastAppliedIndex == cmd.AssignedIndex);

            leader.AppendCommand(cmd);

            Assert.True(waitForCommitsOnCluster.Wait(3000));

            var nodeToRemove = Nodes.First(x => x.State != RaftEngineState.Leader);

            var nodesThatShouldRemain = Nodes.Where(n => ReferenceEquals(n, nodeToRemove) == false)
                                                 .ToList();

            var waitForToplogyChangeOnCluster = WaitForToplogyChangeOnCluster(nodesThatShouldRemain);
            leader.RemoveFromClusterAsync(new NodeConnectionInfo { Name = nodeToRemove.Name }).Wait();

            Assert.True(waitForToplogyChangeOnCluster.Wait(5000));


            var nodePeerLists = Nodes.Where(n => ReferenceEquals(n, nodeToRemove) == false)
                                         .Select(n => n)
                                         .ToList();

            foreach (var node in nodePeerLists)
            {
                Assert.Equal(nodesThatShouldRemain.Count(), node.CurrentTopology.AllNodeNames.Count());

                foreach (var n in nodesThatShouldRemain)
                {
                    Assert.True(node.CurrentTopology.Contains(n.Name));
                }
            }
        }


        [Fact]
        public void Cluster_nodes_are_able_to_recover_after_shutdown_in_the_middle_of_topology_change()
        {
            var leader = CreateNetworkAndGetLeader(2);
            var nonLeader = Nodes.First(x => x != leader);
            var topologyChangeStarted = new ManualResetEventSlim();
            nonLeader.TopologyChanging += () =>
            {
                Console.WriteLine("<---disconnected from sending : " + nonLeader.Name);
                DisconnectNodeSending(nonLeader.Name);
                topologyChangeStarted.Set();
            };
            leader.AddToClusterAsync(new NodeConnectionInfo { Name = "nodeC" });
            Assert.True(topologyChangeStarted.Wait(2000));

            RestartAllNodes();

            WriteLine("<---nodeA, nodeB are down");

            ReconnectNodeSending(nonLeader.Name);
            var topologyChangesFinished = WaitForToplogyChangeOnCluster();
            Assert.True(topologyChangesFinished.Wait(3000));

            foreach (var raftEngine in Nodes)
            {
                raftEngine.CurrentTopology.AllNodeNames.Should().Contain("nodeC");
            }
        }

        [Fact]
        public void Cluster_cannot_have_two_concurrent_node_additions()
        {
            var leader = CreateNetworkAndGetLeader(4, messageTimeout: 1500);

            leader.AddToClusterAsync(new NodeConnectionInfo { Name = "extra1" });

            //if another removal from cluster is in progress, 
            Assert.Throws<InvalidOperationException>(() => leader.AddToClusterAsync(new NodeConnectionInfo { Name = "extra2" }).Wait());
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(7)]
        public void Node_added_to_cluster_should_update_peers_list(int nodeCount)
        {
            WriteLine("--> Started test");
            var leader = CreateNetworkAndGetLeader(nodeCount);
            WriteLine("--> Selected leader, creating additional node");
            using (var additionalNode = NewNodeFor(leader))
            {
                var clusterChanged = WaitForToplogyChangeOnCluster();
                var newNodeAdded = WaitForNodeToBecomeVoter(additionalNode);

                WriteLine("Adding the additional node (name = {0}) to cluster", additionalNode.Name);
                leader.AddToClusterAsync(new NodeConnectionInfo { Name = additionalNode.Name }).Wait();

                clusterChanged.Wait();
                var becameVoter = newNodeAdded.Wait(TimeSpan.FromSeconds((nodeCount + 1)*10));
                if (!becameVoter)
                {
                    Assert.True(false,"New node never became a voter");
                }
                WriteLine("--> Cluster finished changing, new node added.");

                var raftNodes = Nodes.ToList();
                foreach (var node in raftNodes)
                {
                    var containedInAllVotingNodes = node.CurrentTopology.Contains(additionalNode.Name);
                    if(containedInAllVotingNodes)
                        continue;
                    Assert.True(containedInAllVotingNodes,
                        node.CurrentTopology + " on " + node.Name);
                }

                additionalNode.CurrentTopology.AllNodeNames.Should().Contain(raftNodes.Select(node => node.Name));
            }
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public void Can_step_down(int nodeCount)
        {
            var leader = CreateNetworkAndGetLeader(nodeCount);

            var firstCommits = WaitForCommitsOnCluster(x => x.Data.ContainsKey("4"));
            for (int i = 0; i < 5; i++)
            {
                leader.AppendCommand(new DictionaryCommand.Set
                {
                    Key = i.ToString(),
                    Value = i
                });
            }
            firstCommits.Wait();

            var nextCommit = WaitForCommitsOnCluster(x => x.Data.ContainsKey("c"));

            leader.AppendCommand(new DictionaryCommand.Set
            {
                Key = "c",
                Value = 3
            });

            var newLeader = WaitForNewLeaderAsync();

            leader.StepDownAsync().Wait();

            nextCommit.Wait();

            var dictionaryStateMachine = ((DictionaryStateMachine)newLeader.Result.StateMachine);
            WriteLine("<-- have new leader state machine");

            Assert.Equal(3, dictionaryStateMachine.Data["c"]);
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public void Leader_removed_from_cluster_modifies_member_lists_on_remaining_nodes(int nodeCount)
        {
            try
            {
                var leader = CreateNetworkAndGetLeader(nodeCount);
                var raftNodes = Nodes.ToList();
                var nonLeaderNode = raftNodes.FirstOrDefault(n => n.State != RaftEngineState.Leader);
                Assert.NotNull(leader);
                Assert.NotNull(nonLeaderNode);

                raftNodes.Remove(leader);
                var waitForNewLeaderAsync = WaitForNewLeaderAsync();

                leader.StepDownAsync().Wait();

                var waitForToplogyChangeOnCluster = WaitForToplogyChangeOnCluster(raftNodes);

                waitForNewLeaderAsync.Result.RemoveFromClusterAsync(new NodeConnectionInfo {Name = leader.Name}).Wait();

                Assert.True(waitForToplogyChangeOnCluster.Wait(300));

                var expectedNodeNameList = raftNodes.Select(x => x.Name).ToList();

                raftNodes.ForEach(node => node.CurrentTopology.AllNodeNames.Should()
                    .BeEquivalentTo(expectedNodeNameList, "node " + node.Name + " should have expected AllVotingNodes list"));
            }
            finally
            {
                ReleaseAllNodes();
            }
        }

        [Fact]
        public void Follower_removed_from_cluster_does_not_affect_leader_and_commits()
        {
            var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(5)
                        .All()
                        .With(x => x.Completion = null)
                        .Build()
                        .ToList();

            var leader = CreateNetworkAndGetLeader(4, messageTimeout: 1500);

            var nonLeaderNode = Nodes.First(x => x.State != RaftEngineState.Leader);
            var someCommitsAppliedEvent = new CountdownEvent(2);
            nonLeaderNode.CommitApplied += (cmd) =>
            {
                if (cmd is DictionaryCommand.Set && someCommitsAppliedEvent.CurrentCount > 0)
                    someCommitsAppliedEvent.Signal();
            };

            leader.AppendCommand(commands[0]);
            leader.AppendCommand(commands[1]);

            Assert.True(someCommitsAppliedEvent.Wait(2000));

            Assert.Equal(3, leader.CurrentTopology.QuorumSize);
            WriteLine(string.Format("<--- Removing from cluster {0} --->", nonLeaderNode.Name));
            leader.RemoveFromClusterAsync(new NodeConnectionInfo { Name = nonLeaderNode.Name }).Wait();

            var otherNonLeaderNode = Nodes.First(x => x.State != RaftEngineState.Leader && !ReferenceEquals(x, nonLeaderNode));

            var allCommitsAppliedEvent = new ManualResetEventSlim();
            otherNonLeaderNode.CommitApplied += (cmd) =>
            {
                if (commands[4].AssignedIndex== cmd.AssignedIndex)
                    allCommitsAppliedEvent.Set();
            };


            WriteLine(string.Format("<--- Appending remaining commands ---> (leader name = {0}, reading from {1})", leader.Name, nonLeaderNode.Name));
            leader.AppendCommand(commands[2]);
            leader.AppendCommand(commands[3]);
            leader.AppendCommand(commands[4]);

            var condition = allCommitsAppliedEvent.Wait(3000);
            Assert.True(condition);

            var committedCommands = otherNonLeaderNode.PersistentState.LogEntriesAfter(0).Select(x => nonLeaderNode.PersistentState.CommandSerializer.Deserialize(x.Data))
                                                                                         .OfType<DictionaryCommand.Set>().ToList();
            committedCommands.Should().HaveCount(5);
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal(commands[i].Value, committedCommands[i].Value);
                Assert.Equal(commands[i].AssignedIndex, committedCommands[i].AssignedIndex);
            }

            otherNonLeaderNode.CommitIndex.Should().Be(leader.CommitIndex, "after all commands have been committed, on non-leader nodes should be the same commit index as on index node");
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        public void Follower_removed_from_cluster_modifies_member_lists_on_remaining_nodes(int nodeCount)
        {
            var leader = CreateNetworkAndGetLeader(nodeCount);
            var raftNodes = Nodes.ToList();
            var removedNode = raftNodes.FirstOrDefault(n => n.State != RaftEngineState.Leader);
            var nonLeaderNode = raftNodes.FirstOrDefault(n => n.State != RaftEngineState.Leader && !ReferenceEquals(n, removedNode));
            Assert.NotNull(leader);
            Assert.NotNull(removedNode);

            Trace.WriteLine(string.Format("<-- Leader chosen: {0} -->", leader.Name));
            Trace.WriteLine(string.Format("<-- Node to be removed: {0} -->", removedNode.Name));

            raftNodes.Remove(removedNode);
            var topologyChangeComittedEvent = new CountdownEvent(nodeCount - 1);

            raftNodes.ForEach(node => node.TopologyChanged += cmd => topologyChangeComittedEvent.Signal());

            Trace.WriteLine(string.Format("<-- Removing {0} from the cluster -->", removedNode.Name));
            leader.RemoveFromClusterAsync(new NodeConnectionInfo { Name = removedNode.Name }).Wait();

            Assert.True(topologyChangeComittedEvent.Wait(nodeCount * 2500));

            var expectedNodeNameList = raftNodes.Select(x => x.Name).ToList();
            Trace.WriteLine("<-- expectedNodeNameList:" + expectedNodeNameList.Aggregate(String.Empty, (all, curr) => all + ", " + curr));
            raftNodes.ForEach(node => node.CurrentTopology.AllNodeNames.Should().BeEquivalentTo(expectedNodeNameList, "node " + node.Name + " should have expected AllVotingNodes list"));
        }
    }
}
