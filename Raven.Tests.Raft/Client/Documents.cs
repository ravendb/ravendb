// -----------------------------------------------------------------------
//  <copyright file="Documents.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Cluster;

using Raven.Json.Linq;

using Xunit.Extensions;

namespace Raven.Tests.Raft.Client
{
	public class Documents : RaftTestBase
	{
		[Theory]
		[PropertyData("Nodes")]
		public void PutShouldBePropagated(int numberOfNodes)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader);

			SetupClusterConfiguration(clusterStores);

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), "keys/" + i));
			}
		}

		[Theory]
		[PropertyData("Nodes")]
		public void DeleteShouldBePropagated(int numberOfNodes)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader);

			SetupClusterConfiguration(clusterStores);

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), "keys/" + i));
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Delete("keys/" + i, null);
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitForDelete(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), "keys/" + i));
			}
		}
	}
}