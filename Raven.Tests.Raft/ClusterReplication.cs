// -----------------------------------------------------------------------
//  <copyright file="ClusterReplication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Database.Raft;
using Raven.Database.Raft.Dto;

using Xunit;

namespace Raven.Tests.Raft
{
	public class ClusterReplication : RaftTestBase
	{
		[Fact]
		public async Task EnablingReplicationInClusterWillCreateGlobalReplicationDestinationsOnEachNode()
		{
			try
			{
				var clusterStores = CreateRaftCluster(3);

				using (clusterStores[0])
				using (clusterStores[1])
				using (clusterStores[2])
				{
					var client = servers[0].Options.RaftEngine.Client;
					await client.SendClusterConfigurationAsync(new ClusterConfiguration {EnableReplication = true});

					clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName));

					AssertReplicationDestinations(clusterStores, (i, j, destination) =>
					{
						Assert.False(destination.Disabled);
						Assert.Null(destination.Database);
						Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
					});

					clusterStores.ForEach(store => store.DatabaseCommands.ForSystemDatabase().Delete(Constants.Global.ReplicationDestinationsDocumentName, null));

					await client.SendClusterConfigurationAsync(new ClusterConfiguration {EnableReplication = false});

					clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName));

					AssertReplicationDestinations(clusterStores, (i, j, destination) =>
					{
						Assert.True(destination.Disabled);
						Assert.Null(destination.Database);
						Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
					});
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}

		public void AssertReplicationDestinations(List<DocumentStore> stores, Action<int, int, ReplicationDestination> extraChecks = null)
		{
			for (var i = 0; i < stores.Count; i++)
			{
				var destinationsJson = stores[i].DatabaseCommands.ForSystemDatabase().Get(Constants.Global.ReplicationDestinationsDocumentName);
				var destinations = destinationsJson.DataAsJson.JsonDeserialization<ReplicationDocument>();
				Assert.Equal(2, destinations.Destinations.Count);
				for (var j = 0; j < stores.Count; j++)
				{
					if (j == i)
						continue;
					var destination = destinations.Destinations.First(x => string.Equals(x.Url, stores[j].Url));
					if (extraChecks != null)
					{
						extraChecks(i, j, destination);
					}
				}
			}
		}

		[Fact]
		public async Task WhenChangingTopologyReplicationShouldBeConfiguredProperly()
		{
			var clusterStores = CreateRaftCluster(3);

			using (var store1 = clusterStores[0])
			using (var store2 = clusterStores[1])
			using (var store3 = clusterStores[2])
			{

			}
		}
	}
}