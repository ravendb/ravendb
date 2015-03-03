// -----------------------------------------------------------------------
//  <copyright file="ClusterReplication.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
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
			var clusterStores = CreateRaftCluster(3);

			using (var store1 = clusterStores[0])
			using (var store2 = clusterStores[1])
			using (var store3 = clusterStores[2])
			{
				var client = new RaftHttpClient(servers[0].Options.RaftEngine);
				await client.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = true });

				WaitForDocument(store1.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName);
				WaitForDocument(store2.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName);
				WaitForDocument(store3.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName);

				var destinationsJson = store1.DatabaseCommands.ForSystemDatabase().Get(Constants.Global.ReplicationDestinationsDocumentName);
				var destinations = destinationsJson.DataAsJson.JsonDeserialization<ReplicationDocument>();
				Assert.Equal(2, destinations.Destinations.Count);
				var destination = destinations.Destinations.First(x => string.Equals(x.Url, store2.Url));
				Assert.False(destination.Disabled);
				Assert.Null(destination.Database);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
				destination = destinations.Destinations.First(x => string.Equals(x.Url, store3.Url));
				Assert.False(destination.Disabled);
				Assert.Null(destination.Database);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);

				destinationsJson = store2.DatabaseCommands.ForSystemDatabase().Get(Constants.Global.ReplicationDestinationsDocumentName);
				destinations = destinationsJson.DataAsJson.JsonDeserialization<ReplicationDocument>();
				Assert.Equal(2, destinations.Destinations.Count);
				destination = destinations.Destinations.First(x => string.Equals(x.Url, store1.Url));
				Assert.False(destination.Disabled);
				Assert.Null(destination.Database);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
				destination = destinations.Destinations.First(x => string.Equals(x.Url, store3.Url));
				Assert.False(destination.Disabled);
				Assert.Null(destination.Database);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);

				destinationsJson = store3.DatabaseCommands.ForSystemDatabase().Get(Constants.Global.ReplicationDestinationsDocumentName);
				destinations = destinationsJson.DataAsJson.JsonDeserialization<ReplicationDocument>();
				Assert.Equal(2, destinations.Destinations.Count);
				destination = destinations.Destinations.First(x => string.Equals(x.Url, store1.Url));
				Assert.False(destination.Disabled);
				Assert.Null(destination.Database);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
				destination = destinations.Destinations.First(x => string.Equals(x.Url, store2.Url));
				Assert.False(destination.Disabled);
				Assert.Null(destination.Database);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);

				store1.DatabaseCommands.ForSystemDatabase().Delete(Constants.Global.ReplicationDestinationsDocumentName, null);
				store2.DatabaseCommands.ForSystemDatabase().Delete(Constants.Global.ReplicationDestinationsDocumentName, null);
				store3.DatabaseCommands.ForSystemDatabase().Delete(Constants.Global.ReplicationDestinationsDocumentName, null);

				await client.SendClusterConfigurationAsync(new ClusterConfiguration { EnableReplication = false });

				WaitForDocument(store1.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName);
				WaitForDocument(store2.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName);
				WaitForDocument(store3.DatabaseCommands.ForSystemDatabase(), Constants.Global.ReplicationDestinationsDocumentName);

				destinationsJson = store1.DatabaseCommands.ForSystemDatabase().Get(Constants.Global.ReplicationDestinationsDocumentName);
				destinations = destinationsJson.DataAsJson.JsonDeserialization<ReplicationDocument>();
				Assert.Equal(2, destinations.Destinations.Count);
				destination = destinations.Destinations.First(x => string.Equals(x.Url, store2.Url));
				Assert.True(destination.Disabled);
				Assert.Null(destination.Database);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
				destination = destinations.Destinations.First(x => string.Equals(x.Url, store3.Url));
				Assert.True(destination.Disabled);
				Assert.Null(destination.Database);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);

				destinationsJson = store2.DatabaseCommands.ForSystemDatabase().Get(Constants.Global.ReplicationDestinationsDocumentName);
				destinations = destinationsJson.DataAsJson.JsonDeserialization<ReplicationDocument>();
				Assert.Equal(2, destinations.Destinations.Count);
				destination = destinations.Destinations.First(x => string.Equals(x.Url, store1.Url));
				Assert.True(destination.Disabled);
				Assert.Null(destination.Database);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
				destination = destinations.Destinations.First(x => string.Equals(x.Url, store3.Url));
				Assert.True(destination.Disabled);
				Assert.Null(destination.Database);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);

				destinationsJson = store3.DatabaseCommands.ForSystemDatabase().Get(Constants.Global.ReplicationDestinationsDocumentName);
				destinations = destinationsJson.DataAsJson.JsonDeserialization<ReplicationDocument>();
				Assert.Equal(2, destinations.Destinations.Count);
				destination = destinations.Destinations.First(x => string.Equals(x.Url, store1.Url));
				Assert.True(destination.Disabled);
				Assert.Null(destination.Database);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
				destination = destinations.Destinations.First(x => string.Equals(x.Url, store2.Url));
				Assert.True(destination.Disabled);
				Assert.Null(destination.Database);
				Assert.Equal(TransitiveReplicationOptions.Replicate, destination.TransitiveReplicationBehavior);
			}
		} 
	}
}