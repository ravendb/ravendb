// -----------------------------------------------------------------------
//  <copyright file="Attachments.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

using Raven.Abstractions.Cluster;
using Raven.Json.Linq;

using Xunit.Extensions;

namespace Raven.Tests.Raft.Client
{
	public class Attachments : RaftTestBase
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

				store.DatabaseCommands.PutAttachment("keys/" + i, null, new MemoryStream(), new RavenJObject());
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitFor(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), commands => commands.GetAttachment("keys/" + i) != null));
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

				store.DatabaseCommands.PutAttachment("keys/" + i, null, new MemoryStream(), new RavenJObject());
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitFor(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), commands => commands.GetAttachment("keys/" + i) != null));
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.DeleteAttachment("keys/" + i, null);
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitFor(store.DatabaseCommands.ForDatabase(store.DefaultDatabase, ClusterBehavior.None), commands => commands.GetAttachment("keys/" + i) == null));
			}
		}
	}
}