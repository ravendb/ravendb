// -----------------------------------------------------------------------
//  <copyright file="RaftTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using Rachis.Transport;

using Raven.Client;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Raft;
using Raven.Database.Raft.Util;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.Raft
{
	public class RaftTestBase : RavenTestBase
	{
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
			var port = 8079;

			var nodes = Enumerable.Range(0, numberOfNodes)
				.Select(x => GetNewServer(port--))
				.ToList();

			var allNodesFinishedJoining = new ManualResetEventSlim();

			var random = new Random();
			var leader = nodes[random.Next(0, numberOfNodes - 1)];

			Console.WriteLine("Leader: " + leader.Options.RaftEngine.Engine.Options.SelfConnection.Uri);

			RaftEngineFactory.InitializeTopology(leader.SystemDatabase, leader.Options.RaftEngine);

			Thread.Sleep(5000); // TODO [ppekrol] trial election?
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

			Assert.True(allNodesFinishedJoining.Wait(5000 * numberOfNodes));
			Assert.True(leader.Options.RaftEngine.Engine.WaitForLeader());

			return nodes
				.Select(node => NewRemoteDocumentStore(ravenDbServer: node))
				.ToList();
		}
	}
}