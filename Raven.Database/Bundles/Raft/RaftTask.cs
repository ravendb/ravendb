// -----------------------------------------------------------------------
//  <copyright file="RaftTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Database.Config.Retriever;
using Raven.Database.Plugins;

namespace Raven.Database.Bundles.Raft
{
	//[ExportMetadata("Bundle", "Raft")]
	[InheritedExport(typeof(IStartupTask))]
	public class RaftTask : IStartupTask, IDisposable
	{
		private ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin> lastSeenDocument;

		private RaftHttpClient raftClient;

		protected DocumentDatabase Database { get; set; }

		public void Execute(DocumentDatabase database)
		{
			Database = database;
			raftClient = new RaftHttpClient(Database.RaftEngine);

			Database
				.ConfigurationRetriever
				.SubscribeToConfigurationDocumentChanges(Constants.RavenReplicationDestinations, async () => await HandleChangesAsync());

			Task.Factory.StartNew(async () =>
			{
				await HandleChangesAsync();
			});
		}

		private async Task HandleChangesAsync()
		{
			var document = Database
				.ConfigurationRetriever
				.GetConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>>(Constants.RavenReplicationDestinations);

			await HandleRemovalsAsync(document);
			await HandleAdditionsAsync(document);

			lastSeenDocument = document != null ? document.MergedDocument : null;
		}

		private async Task HandleRemovalsAsync(ConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>> document)
		{
			if (document == null && lastSeenDocument == null)
				return;

			if (document == null && lastSeenDocument != null) // need to leave
			{
				await raftClient.LeaveAsync();
				return;
			}

			if (document != null && lastSeenDocument == null)
				return;

			if (document.MergedDocument.Destinations.Count == 0)
			{
				await raftClient.LeaveAsync();
				return;
			}

			foreach (var oldDestination in lastSeenDocument.Destinations)
			{
				var newDestination = document.MergedDocument.Destinations.SingleOrDefault(x => string.Equals(x.Url, oldDestination.Url));
				if (newDestination == null) // removed
				{
					// is in cluster -> master/master -> leave cluster -> break loop
					// is not in cluster -> master/slave -> no-op
					continue;
				}

				if (newDestination.Disabled && oldDestination.Disabled == false)
				{
					// is in cluster -> master/master -> leave cluster -> break loop
					// is not in cluster -> master/slave -> no-op
				}
			}
		}

		private async Task HandleAdditionsAsync(ConfigurationDocument<ReplicationDocument<ReplicationDestination.ReplicationDestinationWithConfigurationOrigin>> document)
		{
			if (document == null)
				return;

			if (document.MergedDocument.Destinations.Count == 0)
				return;

			foreach (var destination in document.MergedDocument.Destinations)
			{
				if (destination.Disabled)
					continue;

				try
				{
					var url = destination.Url.ForDatabase(destination.Database);

					if (Database.RaftEngine.CurrentTopology.QuorumSize <= 1) // alone, can join others
						await raftClient.JoinAsync(url);
					else 
						await raftClient.JoinMeAsync(url);

					// joined once, we can break
					break;
				}
				catch (Exception)
				{

				}
			}
		}

		public void Dispose()
		{
		}
	}
}