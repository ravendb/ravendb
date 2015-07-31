// -----------------------------------------------------------------------
//  <copyright file="TransformerReplicationTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Bundles.Replication.Impl;
using Raven.Database;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Tasks
{
	public class TransformerReplicationTask : ReplicationTaskBase
	{
		private readonly static ILog Log = LogManager.GetCurrentClassLogger();
		
		private readonly ReplicationTask replication;
		private readonly TimeSpan replicationFrequency;
		private Timer timer;
		private readonly object replicationLock = new object();

		public TransformerReplicationTask(DocumentDatabase database, HttpRavenRequestFactory httpRavenRequestFactory, ReplicationTask replication)
			: base(database, httpRavenRequestFactory)
		{
			this.replication = replication;

			replicationFrequency = TimeSpan.FromSeconds(database.Configuration.IndexAndTransformerReplicationLatencyInSec); //by default 10 min
			TimeToWaitBeforeSendingDeletesOfTransformersToSiblings = TimeSpan.FromMinutes(1);
		}

		public TimeSpan TimeToWaitBeforeSendingDeletesOfTransformersToSiblings { get; set; }

		public void Start()
		{
			database.Notifications.OnTransformerChange += OnTransformerChange;

			timer = database.TimerManager.NewTimer(x => Execute(), TimeSpan.Zero, replicationFrequency);
		}

		private void OnTransformerChange(DocumentDatabase documentDatabase, TransformerChangeNotification eventArgs)
		{
			switch (eventArgs.Type)
			{
				case TransformerChangeTypes.TransformerAdded:
					//if created transformer with the same name as deleted one, we should prevent its deletion replication
					database.TransactionalStorage.Batch(accessor => accessor.Lists.Remove(Constants.RavenReplicationTransformerTombstones, eventArgs.Name));
					break;
				case TransformerChangeTypes.TransformerRemoved:
					var metadata = new RavenJObject
					{
						{Constants.RavenTransformerDeleteMarker, true},
						{Constants.RavenReplicationSource, database.TransactionalStorage.Id.ToString()},
						{Constants.RavenReplicationVersion, ReplicationHiLo.NextId(database)}
					};

					database.TransactionalStorage.Batch(accessor => accessor.Lists.Set(Constants.RavenReplicationTransformerTombstones, eventArgs.Name, metadata, UuidType.Transformers));
					break;
			}
		}

		public bool Execute(Func<ReplicationDestination, bool> shouldSkipDestinationPredicate = null)
		{
			if (database.Disposed)
				return false;

			if (Monitor.TryEnter(replicationLock) == false)
				return false;

			try
			{
				using (CultureHelper.EnsureInvariantCulture())
				{
					shouldSkipDestinationPredicate = shouldSkipDestinationPredicate ?? (x => x.SkipIndexReplication == false);
					var replicationDestinations = replication.GetReplicationDestinations(x => shouldSkipDestinationPredicate(x));

					foreach (var destination in replicationDestinations)
					{
						try
						{
							var now = SystemTime.UtcNow;

							var transformerTombstones = GetTombstones(Constants.RavenReplicationTransformerTombstones, 0, 64,
								// we don't send out deletions immediately, we wait for a bit
								// to make sure that the user didn't reset the index or delete / create
								// things manually
								x => (now - x.CreatedAt) >= TimeToWaitBeforeSendingDeletesOfTransformersToSiblings);
							var replicatedTransformerTombstones = new Dictionary<string, int>();

							ReplicateTransformerDeletionIfNeeded(transformerTombstones, destination, replicatedTransformerTombstones);

							if (database.Transformers.Definitions.Length > 0)
							{
								foreach (var definition in database.Transformers.Definitions)
									ReplicateSingleTransformer(destination, definition);
							}

							database.TransactionalStorage.Batch(actions =>
							{
								foreach (var transformerTombstone in replicatedTransformerTombstones)
								{
									var transfomerExists = database.Transformers.GetTransformerDefinition(transformerTombstone.Key) != null;
									if (transformerTombstone.Value != replicationDestinations.Length &&
									    transfomerExists == false)
										continue;

									actions.Lists.Remove(Constants.RavenReplicationTransformerTombstones, transformerTombstone.Key);
								}
							});
						}
						catch (Exception e)
						{
							Log.ErrorException("Failed to replicate transformers to " + destination, e);
						}

					}
					return true;
				}
			}
			catch (Exception e)
			{
				Log.ErrorException("Failed to replicate transformers", e);

				return false;
			}
			finally
			{
				Monitor.Exit(replicationLock);
			}
		}

		public void Execute(string transformerName)
		{
			var definition = database.IndexDefinitionStorage.GetTransformerDefinition(transformerName);

			if (definition == null)
				return;

			foreach (var destination in replication.GetReplicationDestinations(x => x.SkipIndexReplication == false))
			{
				ReplicateSingleTransformer(destination, definition);
			}
		}

		private void ReplicateSingleTransformer(ReplicationStrategy destination, TransformerDefinition definition)
		{
			try
			{
				var clonedTransformer = definition.Clone();
				clonedTransformer.TransfomerId = 0;

				var url = destination.ConnectionStringOptions.Url + "/transformers/" + Uri.EscapeUriString(definition.Name) + "?" + GetDebugInformation();
                var replicationRequest = httpRavenRequestFactory.Create(url, HttpMethod.Put, destination.ConnectionStringOptions, replication.GetRequestBuffering(destination));
				replicationRequest.Write(RavenJObject.FromObject(clonedTransformer));
				replicationRequest.ExecuteRequest();
			}
			catch (Exception e)
			{
				replication.HandleRequestBufferingErrors(e, destination);

				Log.WarnException("Could not replicate transformer " + definition.Name + " to " + destination.ConnectionStringOptions.Url, e);
			}
		}

		private void ReplicateTransformerDeletionIfNeeded(List<JsonDocument> transformerTombstones, ReplicationStrategy destination, Dictionary<string, int> replicatedTransformerTombstones)
		{
			if (transformerTombstones.Count == 0)
				return;

			foreach (var tombstone in transformerTombstones)
			{
				try
				{
					int value;
					if (database.Transformers.GetTransformerDefinition(tombstone.Key) != null) //if in the meantime the transformer was recreated under the same name
					{
						replicatedTransformerTombstones.TryGetValue(tombstone.Key, out value);
						replicatedTransformerTombstones[tombstone.Key] = value + 1;
						continue;
					}

					var url = string.Format("{0}/transformers/{1}?{2}", destination.ConnectionStringOptions.Url, Uri.EscapeUriString(tombstone.Key), GetDebugInformation());
					var replicationRequest = httpRavenRequestFactory.Create(url, HttpMethods.Delete, destination.ConnectionStringOptions, replication.GetRequestBuffering(destination));
					replicationRequest.Write(RavenJObject.FromObject(emptyRequestBody));
					replicationRequest.ExecuteRequest();
					Log.Info("Replicated transformer deletion (transformer name = {0})", tombstone.Key);
					replicatedTransformerTombstones.TryGetValue(tombstone.Key, out value);
					replicatedTransformerTombstones[tombstone.Key] = value + 1;
				}
				catch (Exception e)
				{
					replication.HandleRequestBufferingErrors(e, destination);

					Log.ErrorException(string.Format("Failed to replicate transformer deletion (transformer name = {0})", tombstone.Key), e);
				}
			}
		}

		private string GetDebugInformation()
		{
			return Constants.IsReplicatedUrlParamName + "=true&from=" + Uri.EscapeDataString(database.ServerUrl);
		}

		public override void Dispose()
		{
			if (timer != null)
				timer.Dispose();
		}
	}
}