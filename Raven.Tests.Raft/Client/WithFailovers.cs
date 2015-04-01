// -----------------------------------------------------------------------
//  <copyright file="WithFailovers.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Abstractions.Cluster;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Database.Raft.Util;
using Raven.Json.Linq;

using Xunit.Extensions;

namespace Raven.Tests.Raft.Client
{
	public class WithFailovers : RaftTestBase
	{
		[Theory]
		[InlineData(3)]
		[InlineData(5)]
		public void ReadFromLeaderWriteToLeaderWithFailoversShouldWork(int numberOfNodes)
		{
			WithFailoversInternal(numberOfNodes, ClusterBehavior.ReadFromLeaderWriteToLeaderWithFailovers);
		}

		[Theory]
		[InlineData(3)]
		[InlineData(5)]
		public void ReadFromAllWriteToLeaderWithFailoversShouldWork(int numberOfNodes)
		{
			WithFailoversInternal(numberOfNodes, ClusterBehavior.ReadFromAllWriteToLeaderWithFailovers);
		}

		private void WithFailoversInternal(int numberOfNodes, ClusterBehavior clusterBehavior)
		{
			var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: s => s.Conventions.ClusterBehavior = clusterBehavior);

			SetupClusterConfiguration(clusterStores);

			clusterStores.ForEach(store => ((ServerClient)store.DatabaseCommands).RequestExecuter.UpdateReplicationInformationIfNeededAsync((AsyncServerClient)store.AsyncDatabaseCommands, force: true));

			for (int i = 0; i < clusterStores.Count; i++)
			{
				var store = clusterStores[i];

				store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
			}

			for (int i = 0; i < clusterStores.Count; i++)
			{
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + i));
			}

			servers.First(x => x.Options.ClusterManager.Value.IsLeader()).Dispose();

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