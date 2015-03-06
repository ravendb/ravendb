// -----------------------------------------------------------------------
//  <copyright file="RaftWithAuthn.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rachis.Transport;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Config;
using Raven.Database.Raft;
using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Server;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Raft
{
	public class RaftWithAuthn : RavenTestBase
	{
		protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
		{
			Authentication.EnableOnce();
		}

		[Fact]
		public async Task CanCreateClusterWithApiKeyAndSendCommandToLeader()
		{
			NodeConnectionInfo leaderNci;
			var leader = CreateServerWithOAuth(8079, "Ayende/abc", out leaderNci);

			ClusterManagerFactory.InitializeTopology(leaderNci, leader.Options.ClusterManager);
			Assert.True(leader.Options.ClusterManager.Engine.WaitForLeader());

			NodeConnectionInfo secondConnectionInfo;
			CreateServerWithOAuth(8078, "Marcin/cba", out secondConnectionInfo);
			Assert.True(leader.Options.ClusterManager.Engine.AddToClusterAsync(secondConnectionInfo).Wait(3000));

			NodeConnectionInfo thirdConnectionInfo;
			CreateServerWithOAuth(8077, "User3/pass", out thirdConnectionInfo);
			Assert.True(leader.Options.ClusterManager.Engine.AddToClusterAsync(thirdConnectionInfo).Wait(3000));

			Assert.True(servers[0].Options.ClusterManager.IsLeader());
			var client = servers[0].Options.ClusterManager.Client;
			await client.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = true });

			stores.ForEach(store =>
			{
				WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
				var configurationJson = store.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
				var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
				Assert.True(configuration.EnableReplication);
			});
		}

		[Fact]
		public async Task CanCreateClusterWithApiKeyAndSendCommandToNonLeader()
		{
			NodeConnectionInfo leaderNci;
			var leader = CreateServerWithOAuth(8079, "Ayende/abc", out leaderNci);

			ClusterManagerFactory.InitializeTopology(leaderNci, leader.Options.ClusterManager);
			Assert.True(leader.Options.ClusterManager.Engine.WaitForLeader());

			NodeConnectionInfo secondConnectionInfo;
			CreateServerWithOAuth(8078, "Marcin/cba", out secondConnectionInfo);
			Assert.True(leader.Options.ClusterManager.Engine.AddToClusterAsync(secondConnectionInfo).Wait(3000));

			NodeConnectionInfo thirdConnectionInfo;
			CreateServerWithOAuth(8077, "User3/pass", out thirdConnectionInfo);
			Assert.True(leader.Options.ClusterManager.Engine.AddToClusterAsync(thirdConnectionInfo).Wait(3000));

			WaitForClusterToSettle(3);

			Assert.False(servers[1].Options.ClusterManager.IsLeader());
			var client = servers[1].Options.ClusterManager.Client;
			await client.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = false });

			stores.ForEach(store =>
			{
				WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
				var configurationJson = store.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
				var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
				Assert.False(configuration.EnableReplication);
			});
		}

		private void WaitForClusterToSettle(int numberOfNodes)
		{
			servers.ForEach(server =>
			{
				Assert.True(SpinWait.SpinUntil(() =>
				{
					var topology = server.Options.ClusterManager.Engine.CurrentTopology;
					return topology.AllVotingNodes.Count() == numberOfNodes;
				}, TimeSpan.FromSeconds(15)));

			});
		}
	}
}