// -----------------------------------------------------------------------
//  <copyright file="TopologyChangesUnsafeWay.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using FluentAssertions;
using Rachis.Transport;
using Xunit.Extensions;

namespace Rachis.Tests
{
	public class TopologyChangesUnsafeWay : RaftTestsBase
	{
		[Theory]
		[InlineData(3)]
		[InlineData(4)]
		[InlineData(5)]
		public void Can_remove_nodes_from_topology_even_if_there_is_no_quorum__Unsafe_operation(int clusterSize)
		{
			var leader = CreateNetworkAndGetLeader(clusterSize);

			var cmd = new DictionaryCommand.Set
			{
				Key = "a",
				Value = 1
			};

			var waitForCommitsOnCluster = WaitForCommitsOnCluster(machine => machine.LastAppliedIndex == cmd.AssignedIndex);

			leader.AppendCommand(cmd);

			waitForCommitsOnCluster.Wait(3000).Should().BeTrue();
			
			var nodesToShutdown = GetRandomNodes(leader.CurrentTopology.QuorumSize);
			
			foreach (var node in nodesToShutdown)
			{
				DisconnectNode(node.Name);
				ForceTimeout(node.Name);
			}

			Thread.Sleep(5000);

			var runningNodes = Nodes.Except(nodesToShutdown).ToList();

			var newLeaderTask = WaitForNewLeaderAsync();

			foreach (var nodeDown in nodesToShutdown)
			{
				foreach (var nodeRunning in runningNodes)
				{
					nodeRunning.UnsafeOperations.RemoveFromCluster(new NodeConnectionInfo { Name = nodeDown.Name });
				}
			}
			
			newLeaderTask.Wait(TimeSpan.FromSeconds(10)).Should().BeTrue();

			leader = newLeaderTask.Result;
			
			leader.State.Should().Be(RaftEngineState.Leader);

			cmd = new DictionaryCommand.Set
			{
				Key = "b",
				Value = 1
			};

			waitForCommitsOnCluster = WaitForCommitsOnCluster(machine => machine.LastAppliedIndex == cmd.AssignedIndex, runningNodes);

			leader.AppendCommand(cmd);

			waitForCommitsOnCluster.Wait(3000).Should().BeTrue();

			using (var additionalNode = NewNodeFor(leader))
			{
				var waitForToplogyChangeOnCluster = WaitForToplogyChangeOnCluster(runningNodes.Union(new [] {additionalNode}).ToList());

				leader.AddToClusterAsync(new NodeConnectionInfo { Name = additionalNode.Name }).Wait();

				waitForToplogyChangeOnCluster.Wait(3000).Should().BeTrue();

				foreach (var node in runningNodes)
				{
					node.CurrentTopology.Contains(additionalNode.Name).Should().BeTrue();
					additionalNode.CurrentTopology.Contains(node.Name).Should().BeTrue(); // TODO arek - need to wait for 2 topology changed events on this node?
				}
			}
		}
	}
}