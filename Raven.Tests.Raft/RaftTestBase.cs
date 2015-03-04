// -----------------------------------------------------------------------
//  <copyright file="RaftTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Remoting.Channels;
using System.Threading;
using Rachis;
using Rachis.Transport;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Raft;
using Raven.Database.Raft.Util;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Raft
{
	public class RaftTestBase : RavenTestBase
	{
		protected int port = 8079;

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
		

		public List<DocumentStore> CreateRaftCluster(int numberOfNodes)
		{
			var nodes = Enumerable.Range(0, numberOfNodes)
				.Select(x => GetNewServer(port--))
				.ToList();

			var allNodesFinishedJoining = new ManualResetEventSlim();

			var random = new Random();
			var leader = nodes[random.Next(0, numberOfNodes - 1)];

			Console.WriteLine("Leader: " + leader.Options.RaftEngine.Engine.Options.SelfConnection.Uri);

			RaftEngineFactory.InitializeTopology(leader.SystemDatabase, leader.Options.RaftEngine);

			Assert.True(leader.Options.RaftEngine.Engine.WaitForLeader());

			leader.Options.RaftEngine.Engine.TopologyChanged += command =>
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

				Assert.True(leader.Options.RaftEngine.Engine.AddToClusterAsync(new NodeConnectionInfo
																		{
																			Name = RaftHelper.GetNodeName(n.SystemDatabase.TransactionalStorage.Id),
																			Uri = RaftHelper.GetNodeUrl(n.SystemDatabase.Configuration.ServerUrl)
																		}).Wait(3000));
			}

			if (numberOfNodes == 1)
				allNodesFinishedJoining.Set();

			Assert.True(allNodesFinishedJoining.Wait(10000 * numberOfNodes));
			Assert.True(leader.Options.RaftEngine.Engine.WaitForLeader());

			return nodes
				.Select(node => NewRemoteDocumentStore(ravenDbServer: node))
				.ToList();
		}

		public List<DocumentStore> ExtendRaftCluster(int numberOfExtraNodes)
		{
			var leader = servers.FirstOrDefault(server => server.Options.RaftEngine.IsLeader());
			Assert.NotNull(leader);

			var nodes = Enumerable.Range(0, numberOfExtraNodes)
				.Select(x => GetNewServer(port--))
				.ToList();

			var allNodesFinishedJoining = new ManualResetEventSlim();
			leader.Options.RaftEngine.Engine.TopologyChanged += command =>
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

				Assert.True(leader.Options.RaftEngine.Engine.AddToClusterAsync(new NodeConnectionInfo
				{
					Name = RaftHelper.GetNodeName(n.SystemDatabase.TransactionalStorage.Id),
					Uri = RaftHelper.GetNodeUrl(n.SystemDatabase.Configuration.ServerUrl)
				}).Wait(10000));
			}

			Assert.True(allNodesFinishedJoining.Wait(10000 * numberOfExtraNodes));

			return nodes
				.Select(node => NewRemoteDocumentStore(ravenDbServer: node))
				.ToList();
		}

		public void RemoveFromCluster(RavenDbServer serverToRemove)
		{
			var leader = servers.FirstOrDefault(server => server.Options.RaftEngine.IsLeader());
			if (leader == null)
				throw new InvalidOperationException("Leader is currently not present, thus can't remove node from cluster");
			if (leader == serverToRemove)
			{
				leader.Options.RaftEngine.Engine.StepDownAsync().Wait();
			}
			else
			{
				leader.Options.RaftEngine.Engine.RemoveFromClusterAsync(serverToRemove.Options.RaftEngine.Engine.Options.SelfConnection).Wait(10000);
			}
		}
	}
}