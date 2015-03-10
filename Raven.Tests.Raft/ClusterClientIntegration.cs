// -----------------------------------------------------------------------
//  <copyright file="ClusterClientIntegration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Connection.Request;
using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Json.Linq;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Raft
{
	public class ClusterClientIntegration : RaftTestBase
	{
		[Fact]
		public void RequestExecuterShouldDependOnClusterBehavior()
		{
			using (var store = NewRemoteDocumentStore())
			{
				Assert.Equal(ClusterBehavior.None, store.Conventions.ClusterBehavior);

				var client = (ServerClient)store.DatabaseCommands;
				Assert.True(client.RequestExecuter is ReplicationAwareRequestExecuter);

				client = (ServerClient)store.DatabaseCommands.ForSystemDatabase();
				Assert.True(client.RequestExecuter is ReplicationAwareRequestExecuter);

				var defaultClient = (ServerClient)store.DatabaseCommands;
				client = (ServerClient)defaultClient.ForDatabase(store.DefaultDatabase, ClusterBehavior.None);
				Assert.True(client.RequestExecuter is ReplicationAwareRequestExecuter);
				Assert.True(defaultClient == client);

				client = (ServerClient)store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.ReadFromLeaderWriteToLeader);
				Assert.True(client.RequestExecuter is ClusterAwareRequestExecuter);
			}
		}

		[Theory]
		[InlineData(1)]
		[InlineData(3)]
		[InlineData(5)]
		public async Task ClientsShouldBeAbleToPerformCommandsEvenIfTheyDoNotPointToLeader(int numberOfNodes)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader);

			var managementClient = servers[0].Options.ClusterManager.Client;
			await managementClient.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = true });

			clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName));

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + i));
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Put("keys/" + (i + clusterStores.Count), null, new RavenJObject(), new RavenJObject());
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + (i + clusterStores.Count)));
			}
		}

		[Fact]
		public async Task NonClusterCommandsCanPerformCommandsOnClusterServers()
		{
			var clusterStores = CreateRaftCluster(2, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader);

			var managementClient = servers[0].Options.ClusterManager.Client;
			await managementClient.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = true });

			clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName));

			using (var store1 = clusterStores[0])
			using (var store2 = clusterStores[1])
			{
				var nonClusterCommands1 = (ServerClient)store1.DatabaseCommands.ForDatabase(store1.DefaultDatabase, ClusterBehavior.None);
				var nonClusterCommands2 = (ServerClient)store2.DatabaseCommands.ForDatabase(store1.DefaultDatabase, ClusterBehavior.None);

				nonClusterCommands1.Put("keys/1", null, new RavenJObject(), new RavenJObject());
				nonClusterCommands2.Put("keys/2", null, new RavenJObject(), new RavenJObject());

				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/1"));
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/2"));
			}
		}

		[Theory]
		[InlineData(2)]
		//[InlineData(3)]
		//[InlineData(5)]
		public async Task T1(int numberOfNodes)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader);

			var managementClient = servers[0].Options.ClusterManager.Client;
			await managementClient.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = true });

			clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName));

			clusterStores.ForEach(store => ((ServerClient)store.DatabaseCommands).RequestExecuter.UpdateReplicationInformationIfNeeded(force: true));

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + i));
			}

			Thread.Sleep(10000);

			servers
				.First(x => x.Options.ClusterManager.IsLeader())
				.Dispose();

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Put("keys/" + (i + clusterStores.Count), null, new RavenJObject(), new RavenJObject());
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + (i + clusterStores.Count)));
			}
		}
	}
}