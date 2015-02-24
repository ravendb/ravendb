using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FizzWare.NBuilder;
using FluentAssertions;
using Rachis.Storage;
using Rachis.Transport;
using Voron;
using Xunit;
using Xunit.Extensions;

namespace Rachis.Tests
{
	public class ElectionRelatedTests : RaftTestsBase
	{
		[Fact]
		public void Follower_as_a_single_node_becomes_leader_automatically()
		{
			var hub = new InMemoryTransportHub();
			var storageEnvironmentOptions = StorageEnvironmentOptions.CreateMemoryOnly();
			storageEnvironmentOptions.OwnsPagers = false;
			
			var raftEngineOptions = new RaftEngineOptions(
				new NodeConnectionInfo { Name = "node1" }, 
				storageEnvironmentOptions,
				hub.CreateTransportFor("node1"),
				new DictionaryStateMachine()
				)
			{
				ElectionTimeout = 1000,
				HeartbeatTimeout = 1000/6
			};

			PersistentState.ClusterBootstrap(raftEngineOptions);
			storageEnvironmentOptions.OwnsPagers = true;

			using (var raftNode = new RaftEngine(raftEngineOptions))
			{
				Assert.Equal(RaftEngineState.Leader, raftNode.State);
			}
		}


		[Fact]
		public void Network_partition_should_cause_message_resend()
		{
			var leader = CreateNetworkAndGetLeader(3, messageTimeout: 300);
			
			var countdown = new CountdownEvent(2);
			leader.ElectionStarted += () =>
			{
				if (countdown.CurrentCount > 0)
					countdown.Signal();
			};
			WriteLine("Disconnecting network");
			for (int i = 0; i < 3; i++)
			{
				DisconnectNode("node" + i);
				DisconnectNodeSending("node" + i);
			}

			for (int i = 0; i < 5; i++)
			{
				ForceTimeout(leader.Name);

			}
			Assert.True(countdown.Wait(1500));

			for (int i = 0; i < 3; i++)
			{
				ReconnectNode("node" + i);
				ReconnectNodeSending("node" + i);
			}

			Assert.True(Nodes.First().WaitForLeader());
		}

		/*
		 * This test deals with network "partition" -> leader is detached from the rest of the nodes (simulation of network issues)
		 * Before the network is partitioned the leader distributes the first three commands, then the partition happens.
		 * Then the detached leader has 2 more commands appended - but because of network partition, they are not distributed to other nodes
		 * When communication is restored, the leader from before becomes follower, and the new leader makes roll back on log of former leader, 
		 * so only the first three commands are in the log of former leader node
		 */
		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(5)]
		[InlineData(7)]
		public void Network_partition_for_more_time_than_timeout_can_be_healed(int nodeCount)
		{
			const int CommandCount = 5;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount)
				.All()
				.With(x => x.Completion = new TaskCompletionSource<object>())
				.With(x => x.AssignedIndex = -1)
				.Build()
				.ToList();

			var leader = CreateNetworkAndGetLeader(nodeCount);

			var nonLeaderNode = Nodes.First(x => x.State != RaftEngineState.Leader);
			var commitsAppliedEvent = new ManualResetEventSlim();
			nonLeaderNode.CommitIndexChanged += (oldIndex, newIndex) =>
			{
				if (newIndex == commands[2].AssignedIndex)
					commitsAppliedEvent.Set();
			};

			commands.Take(3).ToList().ForEach(leader.AppendCommand);
			var waitForCommitsOnCluster = WaitForCommitsOnCluster(machine => machine.LastAppliedIndex == commands[2].AssignedIndex);
			Assert.True(commitsAppliedEvent.Wait(5000)); //with in-memory transport it shouldn't take more than 5 sec

			var steppedDown = WaitForStateChange(leader, RaftEngineState.FollowerAfterStepDown);
			var candidancies = Nodes.Where(x=>x!=leader).Select(node => WaitForStateChange(node, RaftEngineState.Candidate)).ToArray();

			WriteLine("<Disconnecting leader!> (" + leader.Name + ")");
			DisconnectNode(leader.Name);

			commands.Skip(3).ToList().ForEach(leader.AppendCommand);
			var formerLeader = leader;

			Assert.True(steppedDown.Wait(leader.Options.ElectionTimeout * 2));
			Assert.True(WaitHandle.WaitAny(candidancies.Select(x => x.WaitHandle).ToArray(), leader.Options.ElectionTimeout*2) != WaitHandle.WaitTimeout);
			WriteLine("<Reconnecting leader!> (" + leader.Name + ")");
			ReconnectNode(leader.Name);

			foreach (var raftEngine in Nodes)
			{
				Assert.True(raftEngine.WaitForLeader());
			}
			
			leader = Nodes.FirstOrDefault(x => x.State == RaftEngineState.Leader);
			Assert.NotNull(leader);

			Assert.True(waitForCommitsOnCluster.Wait(3000));
			var committedCommands = formerLeader.PersistentState.LogEntriesAfter(0).Select(x => nonLeaderNode.PersistentState.CommandSerializer.Deserialize(x.Data))
																					.OfType<DictionaryCommand.Set>()
																					.ToList();
			for (int i = 0; i < 3; i++)
			{
				commands[i].Value.Should().Be(committedCommands[i].Value);
				commands[i].AssignedIndex.Should().Be(committedCommands[i].AssignedIndex);
			}
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void Network_partition_for_less_time_than_timeout_can_be_healed_without_elections(int nodeCount)
		{
			const int CommandCount = 5;
			var commands = Builder<DictionaryCommand.Set>.CreateListOfSize(CommandCount)
				.All()
				.With(x => x.Completion = new TaskCompletionSource<object>())
				.With(x => x.AssignedIndex = -1)
				.Build()
				.ToList();

			var leader = CreateNetworkAndGetLeader(nodeCount, messageTimeout: 1500);

			var nonLeaderNode = Nodes.First(x => x.State != RaftEngineState.Leader);

			commands.Take(CommandCount - 1).ToList().ForEach(leader.AppendCommand);
			while (nonLeaderNode.CommitIndex < 2) //make sure at least one command is committed
				Thread.Sleep(50);

			WriteLine("<Disconnecting leader!> (" + leader.Name + ")");
			DisconnectNode(leader.Name);

			DictionaryCommand.Set command = commands.Last();
			leader.AppendCommand(command);

			var waitForCommitsOnCluster = WaitForCommitsOnCluster(machine => machine.LastAppliedIndex == command.AssignedIndex);

			WriteLine("<Reconnecting leader!> (" + leader.Name + ")");
			ReconnectNode(leader.Name);
			Assert.Equal(RaftEngineState.Leader, leader.State);
			Assert.True(waitForCommitsOnCluster.Wait(3000));

			var committedCommands = nonLeaderNode.PersistentState.LogEntriesAfter(0).Select(x => nonLeaderNode.PersistentState.CommandSerializer.Deserialize(x.Data))
																					.OfType<DictionaryCommand.Set>()
																					.ToList();
			for (int i = 0; i < CommandCount; i++)
			{
				commands[i].Value.Should().Be(committedCommands[i].Value);
				commands[i].AssignedIndex.Should().Be(committedCommands[i].AssignedIndex);
			}
		}

		[Theory]
		[InlineData(2)]
		[InlineData(3)]
		public void On_many_node_network_after_leader_establishment_all_nodes_know_who_is_leader(int nodeCount)
		{
			var leader = CreateNetworkAndGetLeader(nodeCount);
			var raftNodes = Nodes.ToList();

			var leadersOfNodes = raftNodes.Select(x => x.CurrentLeader).ToList();

			leadersOfNodes.Should().NotContainNulls("After leader is established, all nodes should know that leader exists");
			leadersOfNodes.Should().OnlyContain(l => l.Equals(leader.Name, StringComparison.InvariantCultureIgnoreCase),
				"after leader establishment, all nodes should know only one, selected leader");
		}

		[Fact]
		public void Follower_on_timeout_should_become_candidate()
		{
			var storageEnvironmentOptions = StorageEnvironmentOptions.CreateMemoryOnly();
			storageEnvironmentOptions.OwnsPagers = false;

			var nodeOptions = new RaftEngineOptions(new NodeConnectionInfo { Name = "real" }, storageEnvironmentOptions, _inMemoryTransportHub.CreateTransportFor("real"), new DictionaryStateMachine());

			PersistentState.SetTopologyExplicitly(nodeOptions,
				new Topology(
					new Guid("355a589b-cadc-463d-a515-5add2ea47205"),
					new[]
					{
						new NodeConnectionInfo {Name = "real"}, new NodeConnectionInfo {Name = "u2"}, new NodeConnectionInfo {Name = "pj"},
					}, new NodeConnectionInfo[0], new NodeConnectionInfo[0]), throwIfTopologyExists: true);
			storageEnvironmentOptions.OwnsPagers = true;

			using (var node = new RaftEngine(nodeOptions))
			{
				var timeoutEvent = new ManualResetEventSlim();
				node.StateTimeout += timeoutEvent.Set;

				ForceTimeout("real");

				timeoutEvent.Wait();
				Assert.Equal(RaftEngineState.Candidate, node.State);
			}
		}

		[Fact]
		public void AllPeers_and_AllVotingPeers_can_be_persistantly_saved_and_loaded()
		{
			var cancellationTokenSource = new CancellationTokenSource();

			var path = "test" + Guid.NewGuid();
			try
			{
				var expectedAllVotingPeers = new List<string> { "Node123", "Node1", "Node2", "NodeG", "NodeB", "NodeABC" };

				using (var options = StorageEnvironmentOptions.ForPath(path))
				{
					using (var persistentState = new PersistentState("self",options, cancellationTokenSource.Token)
					{
						CommandSerializer = new JsonCommandSerializer()
					})
					{
						var currentConfiguration = persistentState.GetCurrentTopology();
						Assert.Empty(currentConfiguration.AllVotingNodes);

						var currentTopology = new Topology(new Guid("355a589b-cadc-463d-a515-5add2ea47205"), 
							expectedAllVotingPeers.Select(x => new NodeConnectionInfo { Name = x }), Enumerable.Empty<NodeConnectionInfo>(), Enumerable.Empty<NodeConnectionInfo>());
						persistentState.SetCurrentTopology(currentTopology, 1);
					}
				}
				using (var options = StorageEnvironmentOptions.ForPath(path))
				{
					using (var persistentState = new PersistentState("self", options, cancellationTokenSource.Token)
					{
						CommandSerializer = new JsonCommandSerializer()
					})
					{
						var currentConfiguration = persistentState.GetCurrentTopology();
						Assert.Equal(expectedAllVotingPeers.Count, currentConfiguration.AllVotingNodes.Count());
						foreach (var nodeConnectionInfo in currentConfiguration.AllVotingNodes)
						{
							Assert.True(expectedAllVotingPeers.Contains(nodeConnectionInfo.Name));
						}
						
					}
				}
			}
			finally
			{
				new DirectoryInfo(path).Delete(true);
			}
		}

		[Fact]
		public void Request_vote_when_leader_exists_will_be_rejected()
		{
			var node = CreateNetworkAndGetLeader(3);

			node.State.Should().Be(RaftEngineState.Leader);



		}
	}
}
