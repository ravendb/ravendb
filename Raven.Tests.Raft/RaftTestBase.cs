// -----------------------------------------------------------------------
//  <copyright file="RaftTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Rachis.Transport;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Raft;
using Raven.Database.Raft.Util;
using Raven.Server;
using Raven.Tests.Helpers;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Xunit;

namespace Raven.Tests.Raft
{
	public class RaftTestBase : RavenTestBase
	{
		private int port = 9000;

		public void WaitForDelete(IDatabaseCommands commands, string key, TimeSpan? timeout = null)
		{
			var done = SpinWait.SpinUntil(() =>
			{
				// We expect to get the doc from the <system> database
				var doc = commands.Get(key);
				return doc == null;
			}, timeout ?? TimeSpan.FromMinutes(5));

			if (!done)
				throw new Exception("WaitForDelete failed");
		}


		public List<DocumentStore> CreateRaftCluster(int numberOfNodes, string activeBundles = null, Action<DocumentStore> configureStore = null, [CallerMemberName] string databaseName = null)
		{
			var nodes = Enumerable.Range(0, numberOfNodes)
				.Select(x => GetNewServer(port--, activeBundles: activeBundles, databaseName: databaseName))
				.ToList();

			var allNodesFinishedJoining = new ManualResetEventSlim();

			var random = new Random();
			var leader = nodes[random.Next(0, numberOfNodes - 1)];

			Console.WriteLine("Leader: " + leader.Options.ClusterManager.Engine.Options.SelfConnection.Uri);

			ClusterManagerFactory.InitializeTopology(leader.Options.ClusterManager);

			Assert.True(leader.Options.ClusterManager.Engine.WaitForLeader());

			leader.Options.ClusterManager.Engine.TopologyChanged += command =>
			{
				if (command.Requested.AllNodeNames.All(command.Requested.IsVoter))
				{
					allNodesFinishedJoining.Set();
				}
			};

			for (var i = 0; i < numberOfNodes; i++)
			{
				var n = nodes[i];

				if (n == leader)
					continue;

				Assert.True(leader.Options.ClusterManager.Engine.AddToClusterAsync(new NodeConnectionInfo
																		{
																			Name = RaftHelper.GetNodeName(n.SystemDatabase.TransactionalStorage.Id),
																			Uri = RaftHelper.GetNodeUrl(n.SystemDatabase.Configuration.ServerUrl)
																		}).Wait(3000));
			}

			if (numberOfNodes == 1)
				allNodesFinishedJoining.Set();

			Assert.True(allNodesFinishedJoining.Wait(10000 * numberOfNodes), "Not all nodes become voters. " + leader.Options.ClusterManager.Engine.CurrentTopology);
			Assert.True(leader.Options.ClusterManager.Engine.WaitForLeader());

			WaitForClusterToBecomeNonStale(nodes);

			return nodes
				.Select(node => NewRemoteDocumentStore(ravenDbServer: node, activeBundles: activeBundles, configureStore: configureStore, databaseName: databaseName))
				.ToList();
		}

		public List<DocumentStore> ExtendRaftCluster(int numberOfExtraNodes)
		{
			var leader = servers.FirstOrDefault(server => server.Options.ClusterManager.IsLeader());
			Assert.NotNull(leader);

			var nodes = Enumerable.Range(0, numberOfExtraNodes)
				.Select(x => GetNewServer(port--))
				.ToList();

			var allNodesFinishedJoining = new ManualResetEventSlim();
			leader.Options.ClusterManager.Engine.TopologyChanged += command =>
			{
				if (command.Requested.AllNodeNames.All(command.Requested.IsVoter))
				{
					allNodesFinishedJoining.Set();
				}
			};

			for (var i = 0; i < numberOfExtraNodes; i++)
			{
				var n = nodes[i];

				if (n == leader)
					continue;

				Assert.True(leader.Options.ClusterManager.Engine.AddToClusterAsync(new NodeConnectionInfo
				{
					Name = RaftHelper.GetNodeName(n.SystemDatabase.TransactionalStorage.Id),
					Uri = RaftHelper.GetNodeUrl(n.SystemDatabase.Configuration.ServerUrl)
				}).Wait(10000));
				Assert.True(allNodesFinishedJoining.Wait(10000));
				allNodesFinishedJoining.Reset();
			}

			return nodes
				.Select(node => NewRemoteDocumentStore(ravenDbServer: node))
				.ToList();
		}

		public void RemoveFromCluster(RavenDbServer serverToRemove)
		{
			var leader = servers.FirstOrDefault(server => server.Options.ClusterManager.IsLeader());
			if (leader == null)
				throw new InvalidOperationException("Leader is currently not present, thus can't remove node from cluster");
			if (leader == serverToRemove)
			{
				leader.Options.ClusterManager.Engine.StepDownAsync().Wait();
			}
			else
			{
				leader.Options.ClusterManager.Engine.RemoveFromClusterAsync(serverToRemove.Options.ClusterManager.Engine.Options.SelfConnection).Wait(10000);
			}
		}

		private void WaitForClusterToBecomeNonStale(IReadOnlyCollection<RavenDbServer> nodes)
		{
			var numberOfNodes = nodes.Count;
			var result = SpinWait.SpinUntil(() => nodes.All(x => x.Options.ClusterManager.Engine.CurrentTopology.AllVotingNodes.Count() == numberOfNodes), TimeSpan.FromSeconds(10));

			if (result == false)
				throw new InvalidOperationException("Cluster is stale.");
		}
	}
}