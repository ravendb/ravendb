// -----------------------------------------------------------------------
//  <copyright file="IndexReplicationTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
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
	public class IndexReplicationTask : ReplicationTaskBase
	{
		private readonly static ILog Log = LogManager.GetCurrentClassLogger();

		private readonly ReplicationTask replication;
		private readonly TimeSpan replicationFrequency;
		private readonly TimeSpan lastQueriedFrequency;
		private readonly object indexReplicationLock = new object();
		private Timer indexReplicationTimer;
		private Timer lastQueriedTimer;

		public IndexReplicationTask(DocumentDatabase database, HttpRavenRequestFactory httpRavenRequestFactory, ReplicationTask replication)
			: base(database, httpRavenRequestFactory)
		{
			this.replication = replication;

			replicationFrequency = TimeSpan.FromSeconds(database.Configuration.IndexAndTransformerReplicationLatencyInSec); //by default 10 min
			lastQueriedFrequency = TimeSpan.FromSeconds(database.Configuration.TimeToWaitBeforeRunningIdleIndexes.TotalSeconds / 2);
			TimeToWaitBeforeSendingDeletesOfIndexesToSiblings = TimeSpan.FromMinutes(1);
		}

		public TimeSpan TimeToWaitBeforeSendingDeletesOfIndexesToSiblings { get; set; }

		public void Start()
		{
			database.Notifications.OnIndexChange += OnIndexChange;

			indexReplicationTimer = database.TimerManager.NewTimer(x => Execute(), TimeSpan.Zero, replicationFrequency);
			lastQueriedTimer = database.TimerManager.NewTimer(x => SendLastQueried(), TimeSpan.Zero, lastQueriedFrequency);
		}

		public bool Execute(Func<ReplicationDestination, bool> shouldSkipDestinationPredicate = null)
		{
			if (database.Disposed)
				return false;

			if (Monitor.TryEnter(indexReplicationLock) == false)
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

							var indexTombstones = GetTombstones(Constants.RavenReplicationIndexesTombstones, 0, 64,
								// we don't send out deletions immediately, we wait for a bit
								// to make sure that the user didn't reset the index or delete / create
								// things manually
								x => (now - x.CreatedAt) >= TimeToWaitBeforeSendingDeletesOfIndexesToSiblings);

							var replicatedIndexTombstones = new Dictionary<string, int>();

							ReplicateIndexDeletionIfNeeded(indexTombstones, destination, replicatedIndexTombstones);

							var candidatesForReplication = new List<Tuple<IndexDefinition, IndexingPriority>>();

							if (database.Indexes.Definitions.Length > 0)
							{
								var sideBySideIndexes = database.Indexes.Definitions.Where(x => x.IsSideBySideIndex)
																				 .ToDictionary(x => x.Name, x => x);

								foreach (var indexDefinition in database.Indexes.Definitions.Where(x => !x.IsSideBySideIndex))
								{
									IndexDefinition sideBySideIndexDefinition;
									if (sideBySideIndexes.TryGetValue("ReplacementOf/" + indexDefinition.Name, out sideBySideIndexDefinition))
										ReplicateSingleSideBySideIndex(destination, indexDefinition, sideBySideIndexDefinition);
									else
										candidatesForReplication.Add(Tuple.Create(indexDefinition, database.IndexStorage.GetIndexInstance(indexDefinition.Name).Priority));
								}

								ReplicateIndexesMultiPut(destination, candidatesForReplication);
							}

							database.TransactionalStorage.Batch(actions =>
							{
								foreach (var indexTombstone in replicatedIndexTombstones)
								{
									if (indexTombstone.Value != replicationDestinations.Length && database.IndexStorage.HasIndex(indexTombstone.Key) == false)
									{
										continue;
									}

									actions.Lists.Remove(Constants.RavenReplicationIndexesTombstones, indexTombstone.Key);
								}
							});
						}
						catch (Exception e)
						{
							Log.ErrorException("Failed to replicate indexes to " + destination, e);
						}
					}

					return true;
				}
			}
			catch (Exception e)
			{
				Log.ErrorException("Failed to replicate indexes", e);

				return false;
			}
			finally
			{
				Monitor.Exit(indexReplicationLock);
			}
		}

		public void Execute(string indexName)
		{
			var definition = database.IndexDefinitionStorage.GetIndexDefinition(indexName);

			if (definition == null)
				return;

			if(definition.IsSideBySideIndex)
				return;

			var destinations = replication.GetReplicationDestinations(x => x.SkipIndexReplication == false);

			var sideBySideIndexes = database.Indexes.Definitions.Where(x => x.IsSideBySideIndex).ToDictionary(x => x.Name, x => x);

			IndexDefinition sideBySideIndexDefinition;
			if (sideBySideIndexes.TryGetValue("ReplacementOf/" + definition.Name, out sideBySideIndexDefinition))
			{
				foreach (var destination in destinations)
				{
					ReplicateSingleSideBySideIndex(destination, definition, sideBySideIndexDefinition);
				}
			}
			else
			{
				foreach (var destination in destinations)
				{
					ReplicateIndexesMultiPut(destination, new List<Tuple<IndexDefinition, IndexingPriority>>()
					{
						Tuple.Create(definition, database.IndexStorage.GetIndexInstance(definition.Name).Priority)
					});
				}
			}
		}

		private void OnIndexChange(DocumentDatabase documentDatabase, IndexChangeNotification eventArgs)
		{
			switch (eventArgs.Type)
			{
				case IndexChangeTypes.IndexAdded:
					//if created index with the same name as deleted one, we should prevent its deletion replication
					database.TransactionalStorage.Batch(accessor => accessor.Lists.Remove(Constants.RavenReplicationIndexesTombstones, eventArgs.Name));
					break;
				case IndexChangeTypes.IndexRemoved:
					var metadata = new RavenJObject
					{
						{Constants.RavenIndexDeleteMarker, true},
						{Constants.RavenReplicationSource, database.TransactionalStorage.Id.ToString()},
						{Constants.RavenReplicationVersion, ReplicationHiLo.NextId(database)}
					};

					database.TransactionalStorage.Batch(accessor => accessor.Lists.Set(Constants.RavenReplicationIndexesTombstones, eventArgs.Name, metadata, UuidType.Indexing));
					break;
			}
		}

		private void ReplicateIndexesMultiPut(ReplicationStrategy destination, List<Tuple<IndexDefinition, IndexingPriority>> candidatesForReplication)
		{
			var requestParams = new MultiplePutIndexParam
			{
				Definitions = candidatesForReplication.Select(x => x.Item1).ToArray(),
				IndexesNames = candidatesForReplication.Select(x => x.Item1.Name).ToArray(),
				Priorities = candidatesForReplication.Select(x => x.Item2).ToArray()
			};

			var serializedIndexDefinitions = RavenJToken.FromObject(requestParams);
			var url = string.Format("{0}/indexes?{1}", destination.ConnectionStringOptions.Url, GetDebugInformation());

			var replicationRequest = httpRavenRequestFactory.Create(url, HttpMethods.Put, destination.ConnectionStringOptions, replication.GetRequestBuffering(destination));
			replicationRequest.Write(serializedIndexDefinitions);
			replicationRequest.ExecuteRequest();
		}

		private void ReplicateSingleSideBySideIndex(ReplicationStrategy destination, IndexDefinition indexDefinition, IndexDefinition sideBySideIndexDefinition)
		{
			var url = string.Format("{0}/replication/side-by-side?{1}", destination.ConnectionStringOptions.Url, GetDebugInformation());
			IndexReplaceDocument indexReplaceDocument;

			try
			{
				indexReplaceDocument = database.Documents.Get(Constants.IndexReplacePrefix + sideBySideIndexDefinition.Name, null).DataAsJson.JsonDeserialization<IndexReplaceDocument>();
			}
			catch (Exception e)
			{
				Log.Warn("Cannot get side-by-side index replacement document. Aborting operation. (this exception should not happen and the cause should be investigated)", e);
				return;
			}

			var sideBySideReplicationInfo = new SideBySideReplicationInfo
			{
				Index = indexDefinition,
				SideBySideIndex = sideBySideIndexDefinition,
				OriginDatabaseId = destination.CurrentDatabaseId,
				IndexReplaceDocument = indexReplaceDocument
			};

			var replicationRequest = httpRavenRequestFactory.Create(url, HttpMethod.Post, destination.ConnectionStringOptions, replication.GetRequestBuffering(destination));
			replicationRequest.Write(RavenJObject.FromObject(sideBySideReplicationInfo));
			replicationRequest.ExecuteRequest();
		}

		private void ReplicateIndexDeletionIfNeeded(List<JsonDocument> indexTombstones, ReplicationStrategy destination, Dictionary<string, int> replicatedIndexTombstones)
		{
			if (indexTombstones.Count == 0)
				return;

			foreach (var tombstone in indexTombstones)
			{
				try
				{
					int value;
					if (database.IndexStorage.HasIndex(tombstone.Key)) //if in the meantime the index was recreated under the same name
					{
						replicatedIndexTombstones.TryGetValue(tombstone.Key, out value);
						replicatedIndexTombstones[tombstone.Key] = value + 1;
						continue;
					}

					var url = string.Format("{0}/indexes/{1}?{2}", destination.ConnectionStringOptions.Url, Uri.EscapeUriString(tombstone.Key), GetDebugInformation());
					var replicationRequest = httpRavenRequestFactory.Create(url, HttpMethods.Delete, destination.ConnectionStringOptions, replication.GetRequestBuffering(destination));
					replicationRequest.Write(RavenJObject.FromObject(emptyRequestBody));
					replicationRequest.ExecuteRequest();
					Log.Info("Replicated index deletion (index name = {0})", tombstone.Key);

					replicatedIndexTombstones.TryGetValue(tombstone.Key, out value);
					replicatedIndexTombstones[tombstone.Key] = value + 1;
				}
				catch (Exception e)
				{
					replication.HandleRequestBufferingErrors(e, destination);

					Log.ErrorException(string.Format("Failed to replicate index deletion (index name = {0})", tombstone.Key), e);
				}
			}
		}

		public void SendLastQueried()
		{
			if (database.Disposed)
				return;

			try
			{
				using (CultureHelper.EnsureInvariantCulture())
				{
					var relevantIndexLastQueries = new Dictionary<string, DateTime>();
					var relevantIndexes = database.Statistics.Indexes.Where(indexStats => indexStats.IsInvalidIndex == false && indexStats.Priority != IndexingPriority.Error && indexStats.Priority != IndexingPriority.Disabled && indexStats.LastQueryTimestamp.HasValue);

					foreach (var relevantIndex in relevantIndexes)
					{
						relevantIndexLastQueries[relevantIndex.Name] = relevantIndex.LastQueryTimestamp.GetValueOrDefault();
					}

					if (relevantIndexLastQueries.Count == 0) return;

					var destinations = replication.GetReplicationDestinations(x => x.SkipIndexReplication == false);

					foreach (var destination in destinations)
					{
						try
						{
							string url = destination.ConnectionStringOptions.Url + "/indexes/last-queried";

							var replicationRequest = httpRavenRequestFactory.Create(url, HttpMethods.Post, destination.ConnectionStringOptions, replication.GetRequestBuffering(destination));
							replicationRequest.Write(RavenJObject.FromObject(relevantIndexLastQueries));
							replicationRequest.ExecuteRequest();
						}
						catch (Exception e)
						{
							replication.HandleRequestBufferingErrors(e, destination);

							Log.WarnException("Could not update last query time of " + destination.ConnectionStringOptions.Url, e);
						}
					}
				}
			}
			catch (Exception e)
			{
				Log.ErrorException("Failed to send last queried timestamp of indexes", e);
			}
		}

		public override void Dispose()
		{
			if (indexReplicationTimer != null)
				indexReplicationTimer.Dispose();

			if (lastQueriedTimer != null)
				lastQueriedTimer.Dispose();
		}
	}
}