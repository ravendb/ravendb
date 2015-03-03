// -----------------------------------------------------------------------
//  <copyright file="ClusterKeeper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;

using Rachis;
using Rachis.Commands;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Raft.Dto;
using Raven.Database.Server.Tenancy;
using Raven.Json.Linq;
using Raven.Server;

namespace Raven.Database.Raft
{
	public class ClusterKeeper : IServerStartupTask
	{
		private DocumentDatabase systemDatabase;

		private DatabasesLandlord databaseLandlord;

		private RaftEngine raftEngine;

		public void Execute(RavenDbServer server)
		{
			systemDatabase = server.SystemDatabase;
			databaseLandlord = server.Options.DatabaseLandlord;
			raftEngine = server.Options.RaftEngine;

			systemDatabase.Notifications.OnDocumentChange += (db, notification, metadata) =>
			{
				if (notification.Id != Constants.Cluster.ClusterConfigurationDocumentKey)
					return;

				if (notification.Type != DocumentChangeTypes.Put)
					return;

				HandleClusterConfigurationChanges();
			};

			databaseLandlord.OnDatabaseLoaded += tenantId => HandleClusterConfigurationChangesForDatabase(tenantId);
			raftEngine.TopologyChanged += HandleTopologyChanges;

			HandleClusterConfigurationChanges();
		}

		public void Dispose()
		{
		}

		private void HandleTopologyChanges(TopologyChangeCommand command)
		{
			if (command.Previous == null)
			{
				HandleClusterConfigurationChanges();
				return;
			}

			var removedNodes = command
				.Previous
				.AllNodeNames
				.Except(command.Requested.AllNodeNames, StringComparer.OrdinalIgnoreCase)
				.ToList();

			HandleClusterConfigurationChanges(removedNodes);
		}

		private void HandleClusterConfigurationChanges(List<string> removedNodes = null)
		{
			var nextStart = 0;
			var databases = systemDatabase
				.Documents
				.GetDocumentsWithIdStartingWith(Constants.RavenDatabasesPrefix, null, null, 0, int.MaxValue, systemDatabase.WorkContext.CancellationToken, ref nextStart);

			var databaseIds = databases
				.Select(x => ((RavenJObject)x)["@metadata"])
				.Where(x => x != null)
				.Select(x => x.Value<string>("@id"))
				.Where(x => x != null && x != Constants.SystemDatabase)
				.ToList();

			foreach (var databaseId in databaseIds)
			{
				var key = databaseId;
				if (key.StartsWith(Constants.RavenDatabasesPrefix))
					key = key.Substring(Constants.RavenDatabasesPrefix.Length);

				HandleClusterConfigurationChangesForDatabase(key, removedNodes);
			}
		}

		private void HandleClusterConfigurationChangesForDatabase(string tenantId, List<string> removedNodes = null)
		{
			var configurationJson = systemDatabase.Documents.Get(Constants.Cluster.ClusterConfigurationDocumentKey, null);
			if (configurationJson == null)
				return;

			var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();

			var database = databaseLandlord
				.GetDatabaseInternal(tenantId)
				.ResultUnwrap();

			HandleClusterReplicationChangesForDatabase(database, removedNodes, configuration.EnableReplication);
		}

		private void HandleClusterReplicationChangesForDatabase(DocumentDatabase database, List<string> removedNodes, bool enableReplication)
		{
			var currentTopology = raftEngine.CurrentTopology;
			var replicationDocumentJson = database.Documents.Get(Constants.RavenReplicationDestinations, null);
			var replicationDocument = replicationDocumentJson != null
				? replicationDocumentJson.DataAsJson.JsonDeserialization<ReplicationDocument>()
				: new ReplicationDocument();

			var urls = replicationDocument
				.Destinations
				.Select(x => x.Url.ToLowerInvariant())
				.Concat(currentTopology.AllNodes.Select(x => x.Uri.AbsoluteUri.ToLowerInvariant()))
				.Distinct();

			foreach (var url in urls)
			{
				var destination = replicationDocument.Destinations.FirstOrDefault(x => string.Equals(x.Url, url, StringComparison.OrdinalIgnoreCase));
				var node = currentTopology.AllNodes.FirstOrDefault(x => string.Equals(x.Uri.AbsoluteUri, url, StringComparison.OrdinalIgnoreCase));

				if (destination == null && node == null)
					continue; // not possible, but...

				if (destination != null && node == null)
				{
					if (removedNodes.Contains(destination.Url) == false)
						continue; // external destination

					replicationDocument.Destinations.Remove(destination);
					continue;
				}

				if (string.Equals(node.Name, raftEngine.Options.SelfConnection.Name, StringComparison.OrdinalIgnoreCase))
					continue; // skipping self

				if (destination == null)
				{
					destination = new ReplicationDestination();
					replicationDocument.Destinations.Add(destination);
				}

				destination.ApiKey = node.ApiKey;
				destination.Database = database.Name;
				destination.Disabled = enableReplication == false;
				destination.Domain = node.Domain;
				//destination.Password = node.Password;
				destination.TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate;
				destination.Url = node.Uri.AbsoluteUri;
				destination.Username = node.Username;
			}

			database.Documents.Put(Constants.RavenReplicationDestinations, null, RavenJObject.FromObject(replicationDocument), new RavenJObject(), null);
		}
	}
}