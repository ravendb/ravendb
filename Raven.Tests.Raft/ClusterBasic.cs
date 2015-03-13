// -----------------------------------------------------------------------
//  <copyright file="ClusterBasic.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Raft.Dto;

using Xunit;

namespace Raven.Tests.Raft
{
	public class ClusterBasic : RaftTestBase
	{
		[Fact]
		public void CanCreateClusterAndSendConfiguration()
		{
			var clusterStores = CreateRaftCluster(3);

			SetupClusterConfiguration(clusterStores);

			clusterStores.ForEach(store =>
			{
				WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
				var configurationJson = store.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
				var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
				Assert.True(configuration.EnableReplication);
			});
		}

		[Fact]
		public async Task CanCreateClusterAndModifyConfiguration()
		{
			var clusterStores = CreateRaftCluster(3);

			using (var store1 = clusterStores[0])
			using (var store2 = clusterStores[1])
			using (var store3 = clusterStores[2])
			{
				SetupClusterConfiguration(clusterStores);

				WaitForDocument(store1.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey, TimeSpan.FromSeconds(15));
				WaitForDocument(store2.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey, TimeSpan.FromSeconds(15));
				WaitForDocument(store3.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey, TimeSpan.FromSeconds(15));

				store1.DatabaseCommands.ForSystemDatabase().Delete(Constants.Cluster.ClusterConfigurationDocumentKey, null);
				store2.DatabaseCommands.ForSystemDatabase().Delete(Constants.Cluster.ClusterConfigurationDocumentKey, null);
				store3.DatabaseCommands.ForSystemDatabase().Delete(Constants.Cluster.ClusterConfigurationDocumentKey, null);

				SetupClusterConfiguration(clusterStores, enableReplication: false);

				WaitForDocument(store1.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey, TimeSpan.FromSeconds(15));
				var configurationJson = store1.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
				var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
				Assert.False(configuration.EnableReplication);

				WaitForDocument(store2.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey, TimeSpan.FromSeconds(15));
				configurationJson = store2.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
				configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
				Assert.False(configuration.EnableReplication);

				WaitForDocument(store3.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey, TimeSpan.FromSeconds(15));
				configurationJson = store3.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
				configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
				Assert.False(configuration.EnableReplication);
			}
		}

		[Fact]
		public void CanCreateExtendAndRemoveFromCluster()
		{
			var clusterStores = CreateRaftCluster(3); // 3 nodes

			RemoveFromCluster(servers[1]); // 2 nodes

			ExtendRaftCluster(3); // 5 nodes

			ExtendRaftCluster(2); // 7 nodes

			for (var i = 0; i < servers.Count; i++)
			{
				if (i == 1) // already deleted
					continue;

				RemoveFromCluster(servers[i]);
			}
		}
	}
}