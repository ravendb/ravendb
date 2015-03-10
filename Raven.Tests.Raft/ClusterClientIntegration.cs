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

using Xunit;

namespace Raven.Tests.Raft
{
	public class ClusterClientIntegration : RaftTestBase
	{
		[Fact]
		public async Task ClientsShouldBeAbleToPerformCommandsEvenIfTheyDoNotPointToLeader()
		{
			var clusterStores = CreateRaftCluster(3, activeBundles: "Replication", configureStore: store => store.Conventions.ClusterBehavior = ClusterBehavior.ReadFromLeaderWriteToLeader);

			var managementClient = servers[0].Options.ClusterManager.Client;
			await managementClient.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = true });

			clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName));

			using (var store1 = clusterStores[0])
			using (var store2 = clusterStores[1])
			using (var store3 = clusterStores[2])
			{
				store1.DatabaseCommands.Put("keys/1", null, new RavenJObject(), new RavenJObject());
				store2.DatabaseCommands.Put("keys/2", null, new RavenJObject(), new RavenJObject());
				store3.DatabaseCommands.Put("keys/3", null, new RavenJObject(), new RavenJObject());

				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/1"));
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/2"));
				clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/3"));
			}
		}
	}
}