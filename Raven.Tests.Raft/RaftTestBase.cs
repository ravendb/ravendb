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

using Raven.Client.Document;
using Raven.Database.Raft;
using Raven.Database.Raft.Util;
using Raven.Tests.Helpers;

using Xunit;

namespace Raven.Tests.Raft
{
	public class RaftTestBase : RavenTestBase
	{
		public List<DocumentStore> CreateRaftCluster(int numberOfNodes)
		{
			var port = 8079;

			var nodes = Enumerable.Range(0, numberOfNodes)
				.Select(x => GetNewServer(port--))
				.ToList();

			var allNodesFinishedJoining = new ManualResetEventSlim();

			var random = new Random();
			var leader = nodes[random.Next(0, numberOfNodes - 1)];

			Console.WriteLine("Leader: " + leader.Options.RaftEngine.Options.SelfConnection.Uri);

			RaftEngineFactory.InitializeTopology(leader.SystemDatabase, leader.Options.RaftEngine);

			Thread.Sleep(5000); // TODO [ppekrol] trial election?
			Assert.True(leader.Options.RaftEngine.WaitForLeader());

			leader.Options.RaftEngine.TopologyChanged += command =>
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

				Assert.True(leader.Options.RaftEngine.AddToClusterAsync(new NodeConnectionInfo
																		{
																			Name = RaftHelper.GetNodeName(n.SystemDatabase.TransactionalStorage.Id),
																			Uri = RaftHelper.GetNodeUrl(n.SystemDatabase.Configuration.ServerUrl)
																		}).Wait(3000));
			}

			if (numberOfNodes == 1)
				allNodesFinishedJoining.Set();

			Assert.True(allNodesFinishedJoining.Wait(5000 * numberOfNodes));
			Assert.True(leader.Options.RaftEngine.WaitForLeader());

			return nodes
				.Select(node => NewRemoteDocumentStore(ravenDbServer: node))
				.ToList();
		}
	}
}