// -----------------------------------------------------------------------
//  <copyright file="Basic.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Connection;
using Raven.Database.Raft.Dto;
using Raven.Json.Linq;

using Xunit;

namespace Raven.Tests.Raft
{
	public class Basic : RaftTestBase
	{
		[Fact]
		public async Task CanCreateClusterAndSendConfiguration()
		{
			var clusterStores = CreateRaftCluster(3);

			using (var store1 = clusterStores[0])
			using (var store2 = clusterStores[1])
			using (var store3 = clusterStores[2])
			{
				var request = store1.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, store1.Url + "/admin/raft/commands/cluster/configuration", "PUT", new RavenJObject(), store1.DatabaseCommands.PrimaryCredentials, store1.Conventions));
				await request.WriteAsync(RavenJObject.FromObject(new ClusterConfiguration { EnableReplication = true }));

				WaitForDocument(store1.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
				var configurationJson = store1.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
				var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
				Assert.True(configuration.EnableReplication);

				WaitForDocument(store2.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
				configurationJson = store2.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
				configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
				Assert.True(configuration.EnableReplication);

				WaitForDocument(store3.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
				configurationJson = store3.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
				configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
				Assert.True(configuration.EnableReplication);
			}
		}

		[Fact]
		public async Task CanCreateClusterAndModifyConfiguration()
		{
			var clusterStores = CreateRaftCluster(3);

			using (var store1 = clusterStores[0])
			using (var store2 = clusterStores[1])
			using (var store3 = clusterStores[2])
			{
				var request = store1.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, store1.Url + "/admin/raft/commands/cluster/configuration", "PUT", new RavenJObject(), store1.DatabaseCommands.PrimaryCredentials, store1.Conventions));
				await request.WriteAsync(RavenJObject.FromObject(new ClusterConfiguration { EnableReplication = true }));

				WaitForDocument(store1.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
				WaitForDocument(store2.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
				WaitForDocument(store3.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);

				store1.DatabaseCommands.ForSystemDatabase().Delete(Constants.Cluster.ClusterConfigurationDocumentKey, null);
				store2.DatabaseCommands.ForSystemDatabase().Delete(Constants.Cluster.ClusterConfigurationDocumentKey, null);
				store3.DatabaseCommands.ForSystemDatabase().Delete(Constants.Cluster.ClusterConfigurationDocumentKey, null);

				request = store1.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null, store1.Url + "/admin/raft/commands/cluster/configuration", "PUT", new RavenJObject(), store1.DatabaseCommands.PrimaryCredentials, store1.Conventions));
				await request.WriteAsync(RavenJObject.FromObject(new ClusterConfiguration { EnableReplication = false }));

				WaitForDocument(store1.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
				var configurationJson = store1.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
				var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
				Assert.False(configuration.EnableReplication);

				WaitForDocument(store2.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
				configurationJson = store2.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
				configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
				Assert.False(configuration.EnableReplication);

				WaitForDocument(store3.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
				configurationJson = store3.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
				configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
				Assert.False(configuration.EnableReplication);
			}
		}
	}
}