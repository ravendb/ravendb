// -----------------------------------------------------------------------
//  <copyright file="ClusterKeeperTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

using Rachis.Commands;
using Rachis.Transport;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Database.Commercial;
using Raven.Database.Plugins;
using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Database.Server;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Server;

namespace Raven.Database.Raft
{
    public class ClusterKeeperTask : IServerStartupTask
    {
        private DocumentDatabase systemDatabase;

        private ClusterManager clusterManager;

        public void Execute(RavenDBOptions serverOptions)
        {
            if (IsValidLicense() == false)
                return;

            systemDatabase = serverOptions.SystemDatabase;
            clusterManager = serverOptions.ClusterManager.Value = ClusterManagerFactory.Create(systemDatabase, serverOptions.DatabaseLandlord);

            systemDatabase.Notifications.OnDocumentChange += (db, notification, metadata) =>
            {
                if (string.Equals(notification.Id, Constants.Cluster.ClusterConfigurationDocumentKey, StringComparison.OrdinalIgnoreCase))
                {
                    if (notification.Type != DocumentChangeTypes.Put)
                        return;

                    HandleClusterConfigurationChanges();
                }
            };

            clusterManager.Engine.TopologyChanged += HandleTopologyChanges;

            HandleClusterConfigurationChanges();
        }

        private static bool IsValidLicense()
        {            
            string clustering;
            if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("clustering", out clustering))
            {
                bool active;
                if (bool.TryParse(clustering, out active) && active)
                    return true;
            }

            return ValidateLicense.CurrentLicense.Status.Equals("AGPL - Open Source");
        }

        public void Dispose()
        {
            if(clusterManager != null)
                clusterManager.Engine.TopologyChanged -= HandleTopologyChanges;
        }

        private void HandleTopologyChanges(TopologyChangeCommand command)
        {
            if (RaftHelper.HasDifferentNodes(command) == false)
                return;

            if (command.Previous == null)
            {
                HandleClusterConfigurationChanges();
                return;
            }

            var removedNodeUrls = command
                .Previous
                .AllNodes.Select(x => x.Uri.AbsoluteUri)
                .Except(command.Requested.AllNodes.Select(x => x.Uri.AbsoluteUri))
                .ToList();

            HandleClusterConfigurationChanges(removedNodeUrls);
        }

        private void HandleClusterConfigurationChanges(List<string> removedNodeUrls = null)
        {
            var configurationJson = systemDatabase.Documents.Get(Constants.Cluster.ClusterConfigurationDocumentKey, null);
            if (configurationJson == null)
                return;

            var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
            clusterManager.HandleClusterConfigurationChanged(configuration);
            HandleClusterReplicationChanges(removedNodeUrls, configuration.EnableReplication);
        }

        private void HandleClusterReplicationChanges(List<string> removedNodes, bool enableReplication)
        {
            var currentTopology = clusterManager.Engine.CurrentTopology;
            var replicationDocumentJson = systemDatabase.Documents.Get(Constants.Global.ReplicationDestinationsDocumentName, null);

            var replicationDocument = replicationDocumentJson != null
                ? replicationDocumentJson.DataAsJson.JsonDeserialization<ReplicationDocument>()
                : new ReplicationDocument
                {
                    ClientConfiguration = new ReplicationClientConfiguration
                    {
                        FailoverBehavior = FailoverBehavior.ReadFromLeaderWriteToLeader
                    }
                };

            var replicationDocumentNormalizedDestinations = replicationDocument
                .Destinations
                .ToDictionary(x => RaftHelper.GetNormalizedNodeUrl(x.Url), x => x);

            var currentTopologyNormalizedDestionations = currentTopology
                .AllNodes
                .ToDictionary(x => x.Uri.AbsoluteUri.ToLowerInvariant(), x => x);

            var urls = replicationDocumentNormalizedDestinations
                .Keys
                .Union(currentTopologyNormalizedDestionations.Keys)
                .ToList();

            foreach (var url in urls)
            {
                ReplicationDestination destination;
                replicationDocumentNormalizedDestinations.TryGetValue(url, out destination);
                NodeConnectionInfo node;
                currentTopologyNormalizedDestionations.TryGetValue(url, out node);

                if (destination == null && node == null)
                    continue; // not possible, but...

                if (destination != null && node == null)
                {
                    if (removedNodes != null && removedNodes.Contains(url, StringComparer.OrdinalIgnoreCase))
                    {
                        replicationDocument.Destinations.Remove(destination);
                    }

                    continue;
                }

                if (string.Equals(node.Name, clusterManager.Engine.Options.SelfConnection.Name, StringComparison.OrdinalIgnoreCase))
                    continue; // skipping self

                if (destination == null)
                {
                    destination = new ReplicationDestination();
                    replicationDocument.Destinations.Add(destination);
                }

                destination.ApiKey = node.ApiKey;
                destination.Database = null;
                destination.Disabled = enableReplication == false;
                destination.Domain = node.Domain;
                destination.Password = node.Password;
                destination.TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate;
                destination.SkipIndexReplication = false;
                destination.Url = node.Uri.AbsoluteUri;
                destination.Username = node.Username;
            }

            systemDatabase.Documents.Put(Constants.Global.ReplicationDestinationsDocumentName, null, RavenJObject.FromObject(replicationDocument), new RavenJObject(), null);
        }
    }
}
