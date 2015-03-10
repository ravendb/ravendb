// -----------------------------------------------------------------------
//  <copyright file="ClusterClientIntegration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Data;
using Raven.Database.Raft.Dto;
using Raven.Json.Linq;

using Xunit.Extensions;

namespace Raven.Tests.Raft
{
	public class ClusterClientIntegration : RaftTestBase
	{
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
	}
}