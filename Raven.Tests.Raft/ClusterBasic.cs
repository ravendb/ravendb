// -----------------------------------------------------------------------
//  <copyright file="ClusterBasic.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Raft.Dto;
using Raven.Json.Linq;
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
        public void CanCreateClusterAndSendCustomDatabaseSettings()
        {
            var clusterStores = CreateRaftCluster(3);

            SetupClusterConfiguration(clusterStores, true, new Dictionary<string, string>
            {
                { Constants.MaxClauseCount, "123" }
            });

            clusterStores.ForEach(store =>
            {
                WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
                var configurationJson = store.DatabaseCommands.ForSystemDatabase().Get(Constants.Cluster.ClusterConfigurationDocumentKey);
                var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
                Assert.True(configuration.EnableReplication);
                Assert.Equal("123", configuration.DatabaseSettings[Constants.MaxClauseCount]);
            });
        }

        [Fact]
        public void ClusterWideSettingsArePropatagedToDatabases()
        {
            var clusterStores = CreateRaftCluster(3);
            
            SetupClusterConfiguration(clusterStores, true, new Dictionary<string, string>
            {
                { Constants.MaxClauseCount, "123" }
            });

            clusterStores.ForEach(store =>
            {
                WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Cluster.ClusterConfigurationDocumentKey);
            });

            clusterStores[0].DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("newDB");

            clusterStores.ForEach(store =>
            {
                WaitForDocument(store.DatabaseCommands.ForSystemDatabase(), Constants.Database.Prefix + "newDb");
            });

            clusterStores.ForEach(store =>
            {
                using (var request = store.DatabaseCommands.ForDatabase("newDb").CreateRequest("/debug/config", HttpMethod.Get))
                {
                    var response = request.ReadResponseJson();
                    var jObject = response as RavenJObject;
                    Assert.Equal(123, jObject?.Value<int>("MaxClauseCount"));
                }
            });

        }

        [Fact]
        public void CanCreateClusterAndModifyConfiguration()
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
            var topolofyId = servers.First().Options.ClusterManager.Value.Engine.CurrentTopology.TopologyId;
            RemoveFromCluster(servers[1], topolofyId); // 2 nodes
            WaitForClusterToBecomeNonStale(2);
            ExtendRaftCluster(3, topolofyId); // 5 nodes

            ExtendRaftCluster(2, topolofyId); // 7 nodes
            var removeIndexes = new List<int> {0,2,3,4,5,6};
            var rand = new Random();
            while (removeIndexes.Count>2)
            {
                var popIndex = rand.Next(removeIndexes.Count);
                var popServer = servers[removeIndexes[popIndex]];
                removeIndexes.RemoveAt(popIndex);
                RemoveFromCluster(popServer, topolofyId);
            }
        }
    }
}
