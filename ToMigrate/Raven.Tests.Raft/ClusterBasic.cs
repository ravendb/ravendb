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
                    Assert.Equal(123, jObject.Value<int>("MaxClauseCount"));
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

        [Fact]
        public void CanInitializeNewClusterOnNodeBeingPartOfExistingClusterAndTakeOverNodeFromIt()
        {
            var nodes = CreateRaftCluster(3);

            var selectedNode = 0;

            var nodeClient = nodes[selectedNode];
            var nodeServer = servers[selectedNode];
            var nodeRaftEngine = nodeServer.Options.ClusterManager.Value.Engine;

            var oldClusterId = nodeRaftEngine.CurrentTopology.TopologyId;

            // initialize new single node cluster
            nodeClient.DatabaseCommands.ForSystemDatabase().CreateRequest("/admin/cluster/initialize-new-cluster", new HttpMethod("PATCH")).ExecuteRequest();

            Assert.True(nodeRaftEngine.WaitForLeader());

            var newClusterId = nodeRaftEngine.CurrentTopology.TopologyId;

            Assert.NotEqual(oldClusterId, newClusterId);
            Assert.Equal(1, nodeRaftEngine.CurrentTopology.AllNodes.Count());
            Assert.Contains(nodeRaftEngine.Name, nodeRaftEngine.CurrentTopology.AllNodeNames);

            var nextNodeInNewCluster = 1;
            var nextNodeClient = nodes[nextNodeInNewCluster];
            var newNodeRaftEngine = servers[nextNodeInNewCluster].Options.ClusterManager.Value.Engine;

            // take over the node from existing cluster and join it to a new one
            nodeClient.DatabaseCommands.ForSystemDatabase().CreateRequest("/admin/cluster/join?force=true", new HttpMethod("POST"))
                .WriteWithObjectAsync(newNodeRaftEngine.Options.SelfConnection).Wait();

            // ensure that cluster contains two nodes
            Assert.True(SpinWait.SpinUntil(() => nodeRaftEngine.CurrentTopology.Contains(newNodeRaftEngine.Name), TimeSpan.FromSeconds(30)));
            Assert.True(SpinWait.SpinUntil(() => newNodeRaftEngine.CurrentTopology.Contains(nodeRaftEngine.Name), TimeSpan.FromSeconds(30)));

            // verify that database is 
            nodeClient.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
            {
                Id = "Northwind",
                Settings =
                {
                    {
                        "Raven/DataDir", "~/Databases/Northwind"
                    }
                }
            });

            var key = Constants.Database.Prefix + "Northwind";

            WaitForDocument(nextNodeClient.DatabaseCommands.ForSystemDatabase(), key);
        }
    }
}
