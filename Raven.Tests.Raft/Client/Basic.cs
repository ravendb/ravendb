// -----------------------------------------------------------------------
//  <copyright file="Basic.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Request;
using Raven.Database.Raft.Util;
using Raven.Json.Linq;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Raft.Client
{
	public class Basic : RaftTestBase
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
		[PropertyData("Nodes")]
		public void ClientsShouldBeAbleToPerformCommandsEvenIfTheyDoNotPointToLeader(int numberOfNodes)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader);

			EnableReplicationInCluster(clusterStores);

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
		public void NonClusterCommandsCanPerformCommandsOnClusterServers()
		{
			var clusterStores = CreateRaftCluster(2, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader);

			EnableReplicationInCluster(clusterStores);

			using (var store1 = clusterStores[0])
			using (var store2 = clusterStores[1])
			{
				var nonClusterCommands1 = (ServerClient)store1.DatabaseCommands.ForDatabase(store1.DefaultDatabase, ClusterBehavior.None);
				var nonClusterCommands2 = (ServerClient)store2.DatabaseCommands.ForDatabase(store1.DefaultDatabase, ClusterBehavior.None);

				nonClusterCommands1.Put("keys/1", null, new RavenJObject(), new RavenJObject());
				nonClusterCommands2.Put("keys/2", null, new RavenJObject(), new RavenJObject());

				var allNonClusterCommands = new[] { nonClusterCommands1, nonClusterCommands2 };

				allNonClusterCommands.ForEach(commands => WaitForDocument(commands, "keys/1"));
				allNonClusterCommands.ForEach(commands => WaitForDocument(commands, "keys/2"));
			}
		}

		[Theory]
		[InlineData(3)]
		[InlineData(5)]
		public void ClientShouldHandleLeaderShutdown(int numberOfNodes)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader);

			EnableReplicationInCluster(clusterStores);

			clusterStores.ForEach(store => ((ServerClient)store.DatabaseCommands).RequestExecuter.UpdateReplicationInformationIfNeeded((AsyncServerClient)store.AsyncDatabaseCommands, force: true));

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + i));
			}

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