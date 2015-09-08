// -----------------------------------------------------------------------
//  <copyright file="TopologyChangesUnsafeWay.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Rachis.Transport;
using Xunit;
using Xunit.Extensions;

namespace Rachis.Tests
{
	public class TopologyChangesUnsafeWay : RaftTestsBase
	{
		private RaftEngine leader;
		private List<RaftEngine> nodesToShutdown;
		private List<RaftEngine> runningNodes;

		[Theory]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void can_remove_nodes_from_topology_even_if_there_is_no_majority(int clusterSize)
		{
			CreateCluster(clusterSize);

			ShutDownMajorityOfNodes();

			ForceTimeoutsOnRunningNodes();

			foreach (var nodeDown in nodesToShutdown)
			{
				foreach (var nodeRunning in runningNodes)
				{
					nodeRunning.UnsafeOperations.RemoveFromCluster(new NodeConnectionInfo { Name = nodeDown.Name });
				}
			}

			foreach (var nodeRunning in runningNodes)
			{
				nodesToShutdown.ForEach(x => Assert.False(nodeRunning.CurrentTopology.Contains(x.Name)));
			}
		}

		[Theory]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void can_add_nodes_to_topology_even_if_there_is_no_majority(int clusterSize)
		{
			CreateCluster(clusterSize);

			ShutDownMajorityOfNodes();

			ForceTimeoutsOnRunningNodes();

			foreach (var nodeRunning in runningNodes)
			{
				ForceTimeout(nodeRunning.Name);
			}

			foreach (var nodeRunning in runningNodes)
			{
				nodeRunning.UnsafeOperations.AddToCluster(new NodeConnectionInfo { Name = "new-node" });
			}

			foreach (var nodeRunning in runningNodes)
			{
				Assert.True(nodeRunning.CurrentTopology.Contains("new-node"));
			}
		}

		[Theory]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void new_leader_is_selected_after_removing_dead_nodes(int clusterSize)
		{
			CreateCluster(clusterSize);

			ShutDownMajorityOfNodes();

			ForceTimeoutsOnRunningNodes();

			var newLeaderTask = WaitForNewLeaderAsync();

			foreach (var nodeDown in nodesToShutdown)
			{
				foreach (var nodeRunning in runningNodes)
				{
					nodeRunning.UnsafeOperations.RemoveFromCluster(new NodeConnectionInfo { Name = nodeDown.Name });
				}
			}

			Assert.True(newLeaderTask.Wait(TimeSpan.FromSeconds(10)));

			Assert.Equal(RaftEngineState.Leader, newLeaderTask.Result.State);
		}

		[Theory]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void new_leader_is_selected_after_adding_new_nodes_and_removing_dead_onces(int clusterSize)
		{
			CreateCluster(clusterSize);

			ShutDownMajorityOfNodes();

			ForceTimeoutsOnRunningNodes();

			var newNodes = Enumerable.Range(0, leader.CurrentTopology.QuorumSize).Select(x =>
			{
				Thread.Sleep(new Random().Next(2000));
				return NewNodeFor(leader);
			}).ToList();

			var newLeaderTask = WaitForNewLeaderAsync(runningNodes.Union(newNodes).ToList());

			foreach (var newNode in newNodes)
			{
				foreach (var nodeRunning in runningNodes)
				{
					nodeRunning.UnsafeOperations.AddToCluster(new NodeConnectionInfo { Name = newNode.Name });
				}
			}

			foreach (var nodeDown in nodesToShutdown)
			{
				foreach (var nodeRunning in runningNodes)
				{
					nodeRunning.UnsafeOperations.RemoveFromCluster(new NodeConnectionInfo { Name = nodeDown.Name });
				}
			}

			Assert.True(newLeaderTask.Wait(TimeSpan.FromSeconds(10)));

			Assert.Equal(RaftEngineState.Leader, newLeaderTask.Result.State);

			Assert.True(runningNodes.Contains(newLeaderTask.Result)); // leader should be selected from already running nodes
		}

		[Theory]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void after_retrieving_majority_can_operate_as_usual(int clusterSize)
		{
			CreateCluster(clusterSize);

			ShutDownMajorityOfNodes();

			ForceTimeoutsOnRunningNodes();

			var newLeaderTask = WaitForNewLeaderAsync();

			foreach (var nodeDown in nodesToShutdown)
			{
				foreach (var nodeRunning in runningNodes)
				{
					nodeRunning.UnsafeOperations.RemoveFromCluster(new NodeConnectionInfo { Name = nodeDown.Name });
				}
			}

			Assert.True(newLeaderTask.Wait(TimeSpan.FromSeconds(10)));
			
			leader = newLeaderTask.Result;

			var cmd = new DictionaryCommand.Set
			{
				Key = "b",
				Value = 1
			};

			var waitForCommitsOnCluster = WaitForCommitsOnCluster(machine => machine.LastAppliedIndex == cmd.AssignedIndex, runningNodes);

			leader.AppendCommand(cmd);

			Assert.True(waitForCommitsOnCluster.Wait(3000));

			using (var additionalNode = NewNodeFor(leader))
			{
				var additinalNodeHasAllRunningNodesAndNoDownNodeInTopology = new ManualResetEventSlim();

				additionalNode.TopologyChanged += x =>
				{
					foreach (var runningNode in runningNodes)
					{
						if (x.Requested.Contains(runningNode.Name) == false)
							return;

						if (nodesToShutdown.Any(downNode => x.Requested.Contains(downNode.Name)))
							return;
					}

					additinalNodeHasAllRunningNodesAndNoDownNodeInTopology.Set();
				};

				var additionalNodeAddedToCluster = WaitForToplogyChangeOnCluster(runningNodes);

				leader.AddToClusterAsync(new NodeConnectionInfo { Name = additionalNode.Name }).Wait();

				Assert.True(additionalNodeAddedToCluster.Wait(3000));

				Assert.True(additinalNodeHasAllRunningNodesAndNoDownNodeInTopology.Wait(3000));

				var persistentTopology = additionalNode.PersistentState.GetCurrentTopology();

				foreach (var running in runningNodes)
				{
					Assert.True(persistentTopology.Contains(running.Name));
				}

				foreach (var nodeDown in nodesToShutdown)
				{
					Assert.False(persistentTopology.Contains(nodeDown.Name));
				}

				Assert.True(additionalNode.CurrentTopology.Contains(additionalNode.Name));
				Assert.True(persistentTopology.Contains(additionalNode.Name));
			}
		}

		private void CreateCluster(int clusterSize)
		{
			leader = CreateNetworkAndGetLeader(clusterSize);
		}

		private void ShutDownMajorityOfNodes()
		{
			nodesToShutdown = GetRandomNodes(leader.CurrentTopology.QuorumSize);
			runningNodes = Nodes.Except(nodesToShutdown).ToList();

			foreach (var node in nodesToShutdown)
			{
				DisconnectNode(node.Name);
				node.Dispose();
			}
		}

		private void ForceTimeoutsOnRunningNodes()
		{
			foreach (var nodeRunning in runningNodes)
			{
				ForceTimeout(nodeRunning.Name);
			}
		}
	}
}