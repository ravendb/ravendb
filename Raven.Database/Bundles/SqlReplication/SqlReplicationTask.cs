// -----------------------------------------------------------------------
//  <copyright file="SqlReplicationTask.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Configuration;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Impl.Synchronization;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Server;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Task = System.Threading.Tasks.Task;
using System.Linq;

namespace Raven.Database.Bundles.SqlReplication
{
	[InheritedExport(typeof(IStartupTask))]
	[ExportMetadata("Bundle", "sqlReplication")]
	public class SqlReplicationTask : IStartupTask, IDisposable
	{
		private const string RavenSqlreplicationStatus = "Raven/SqlReplication/Status";
		private readonly static ILog log = LogManager.GetCurrentClassLogger();

		public event Action<int> AfterReplicationCompleted = delegate { };

		public DocumentDatabase Database { get; set; }

		private List<SqlReplicationConfig> replicationConfigs;
		private readonly ConcurrentDictionary<string, SqlReplicationStatistics> statistics = new ConcurrentDictionary<string, SqlReplicationStatistics>(StringComparer.InvariantCultureIgnoreCase);

		private PrefetchingBehavior prefetchingBehavior;

		private EtagSynchronizer etagSynchronizer;
		private Etag lastLatestEtag;

		public void Execute(DocumentDatabase database)
		{
			etagSynchronizer = database.EtagSynchronizer.GetSynchronizer(EtagSynchronizerType.SqlReplicator);
			prefetchingBehavior = database.Prefetcher.GetPrefetchingBehavior(PrefetchingUser.SqlReplicator, null);

			Database = database;
			Database.OnDocumentChange += (sender, notification, metadata) =>
			{
				if (notification.Id == null)
					return;

				if (metadata == null)
					return; // this is a delete being made on an already deleted document

				if (notification.Type == DocumentChangeTypes.Delete)
				{
					RecordDelete(notification.Id, metadata);
				}

				if (!notification.Id.StartsWith("Raven/SqlReplication/Configuration/", StringComparison.InvariantCultureIgnoreCase))
					return;

				replicationConfigs = null;
				statistics.Clear();
				log.Debug(() => "Sql Replication configuration was changed.");
			};

			GetReplicationStatus();

			var task = Task.Factory.StartNew(() =>
			{
				using (LogContext.WithDatabase(database.Name))
				{
					try
					{
						BackgroundSqlReplication();
					}
					catch (Exception e)
					{
						log.ErrorException("Fatal failure when replicating to SQL. All SQL Replication activity STOPPED", e);
					}
				}
			}, TaskCreationOptions.LongRunning);
			database.ExtensionsState.GetOrAdd(typeof(SqlReplicationTask).FullName, k => new DisposableAction(task.Wait));
		}

		private void RecordDelete(string id, RavenJObject metadata)
		{
			Database.TransactionalStorage.Batch(accessor =>
			{
				bool hasChanges = false;
				foreach (var config in replicationConfigs)
				{
					if (string.Equals(config.RavenEntityName, metadata.Value<string>(Constants.RavenEntityName), StringComparison.InvariantCultureIgnoreCase) == false)
						continue;

					hasChanges = true;
					accessor.Lists.Set(GetSqlReplicationDeletionName(config), id, metadata, UuidType.Documents);
				}
				if (hasChanges)
					Database.WorkContext.NotifyAboutWork();
			});
			if (log.IsDebugEnabled)
				log.Debug(() => "recorded a deleted document " + id);
		}

		private SqlReplicationStatus GetReplicationStatus()
		{
			var jsonDocument = Database.Get(RavenSqlreplicationStatus, null);
			return jsonDocument == null
									? new SqlReplicationStatus()
									: jsonDocument.DataAsJson.JsonDeserialization<SqlReplicationStatus>();
		}

		private void BackgroundSqlReplication()
		{
			int workCounter = 0;
			while (Database.WorkContext.DoWork)
			{
				var config = GetConfiguredReplicationDestinations();
				if (config.Count == 0)
				{
					Database.WorkContext.WaitForWork(TimeSpan.FromMinutes(10), ref workCounter, "Sql Replication");
					continue;
				}
				var localReplicationStatus = GetReplicationStatus();

				var relevantConfigs = config.Where(x =>
				{
					if (x.Disabled)
						return false;
					var sqlReplicationStatistics = statistics.GetOrDefault(x.Name);
					if (sqlReplicationStatistics == null)
						return true;
					return SystemTime.UtcNow >= sqlReplicationStatistics.LastErrorTime;
				}) // have error or the timeout expired
						.ToList();

				if (relevantConfigs.Count == 0)
				{
					Database.WorkContext.WaitForWork(TimeSpan.FromMinutes(10), ref workCounter, "Sql Replication");
					continue;
				}

				var leastReplicatedEtag = GetLeastReplicatedEtag(relevantConfigs, localReplicationStatus);

				if (leastReplicatedEtag == null)
				{
					Database.WorkContext.WaitForWork(TimeSpan.FromMinutes(10), ref workCounter, "Sql Replication");
					continue;
				}

				var documents = prefetchingBehavior.GetDocumentsBatchFrom(leastReplicatedEtag);

				Etag latestEtag = null, lastBatchEtag = null;
				if (documents.Count != 0)
					lastBatchEtag = documents[documents.Count - 1].Etag;
				
				var replicationDuration = Stopwatch.StartNew();
				documents.RemoveAll(x => x.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase)); // we ignore system documents here
				
				if (documents.Count != 0)
					latestEtag = documents[documents.Count - 1].Etag;
				
				var deletedDocsByConfig = new Dictionary<SqlReplicationConfig, List<ListItem>>();

				foreach (var relevantConfig in relevantConfigs)
				{
					var cfg = relevantConfig;
					Database.TransactionalStorage.Batch(accessor =>
					{
						deletedDocsByConfig[cfg] = accessor.Lists.Read(GetSqlReplicationDeletionName(cfg),
														  GetLastEtagFor(localReplicationStatus, cfg),
														  latestEtag,
														  1024)
											  .ToList();
					});
				}

				// No documents AND there aren't any deletes to replicate
				if (documents.Count == 0 && deletedDocsByConfig.Sum(x => x.Value.Count) == 0)
				{
					if (latestEtag != null)
					{
						// so we filtered some documents, let us update the etag about that.
						foreach (var lastReplicatedEtag in localReplicationStatus.LastReplicatedEtags)
						{
							if (lastReplicatedEtag.LastDocEtag.CompareTo(latestEtag) <= 0)
								lastReplicatedEtag.LastDocEtag = latestEtag;
						}

						latestEtag = Etag.Max(latestEtag, lastBatchEtag);
						SaveNewReplicationStatus(localReplicationStatus, latestEtag);
					}
					else // no point in waiting if we just saved a new doc
					{
						Database.WorkContext.WaitForWork(TimeSpan.FromMinutes(10), ref workCounter, "Sql Replication");
					}
					continue;
				}

				var successes = new ConcurrentQueue<Tuple<SqlReplicationConfig, Etag>>();
				try
				{
					BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(Database.WorkContext, relevantConfigs, replicationConfig =>
					{
						try
						{
							var lastReplicatedEtag = GetLastEtagFor(localReplicationStatus, replicationConfig);

							var deletedDocs = deletedDocsByConfig[replicationConfig];
							var docsToReplicate = documents
								.Where(x => lastReplicatedEtag.CompareTo(x.Etag) <= 0) // haven't replicate the etag yet
								.ToList();

							var currentLatestEtag = HandleDeletesAndChangesMerging(deletedDocs, docsToReplicate);

							if (ReplicateDeletionsToDestination(replicationConfig, deletedDocs) &&
								ReplicateChangesToDesintation(replicationConfig, docsToReplicate))
							{
								if (deletedDocs.Count > 0)
								{
									Database.TransactionalStorage.Batch(accessor =>
										accessor.Lists.RemoveAllBefore(GetSqlReplicationDeletionName(replicationConfig), deletedDocs[deletedDocs.Count - 1].Etag));
								}
								successes.Enqueue(Tuple.Create(replicationConfig, currentLatestEtag));
							}
						}
						catch (Exception e)
						{
							log.WarnException("Error while replication to SQL destination: " + replicationConfig.Name, e);
							Database.AddAlert(new Alert
							{
								AlertLevel = AlertLevel.Error,
								CreatedAt = SystemTime.UtcNow,
								Exception = e.ToString(),
								Title = "Sql Replication failure to replication",
								Message = "Sql Replication could not replicate to " + replicationConfig.Name,
								UniqueKey = "Sql Replication could not replicate to " + replicationConfig.Name
							});
						}
					});
					if (successes.Count == 0)
						continue;
					foreach (var t in successes)
					{
						var cfg = t.Item1;
						var currentLatestEtag = t.Item2;
						var destEtag = localReplicationStatus.LastReplicatedEtags.FirstOrDefault(x => string.Equals(x.Name, cfg.Name, StringComparison.InvariantCultureIgnoreCase));
						if (destEtag == null)
						{
							localReplicationStatus.LastReplicatedEtags.Add(new LastReplicatedEtag
							{
								Name = cfg.Name,
								LastDocEtag = currentLatestEtag ?? Etag.Empty
							});
						}
						else
						{
							destEtag.LastDocEtag = currentLatestEtag = currentLatestEtag ?? destEtag.LastDocEtag;
						}
						latestEtag = Etag.Max(latestEtag, currentLatestEtag);
					}

					latestEtag = Etag.Max(latestEtag, lastBatchEtag);
					SaveNewReplicationStatus(localReplicationStatus, latestEtag);
				}
				finally
				{
					AfterReplicationCompleted(successes.Count);
					var min = localReplicationStatus.LastReplicatedEtags.Min(x => new ComparableByteArray(x.LastDocEtag.ToByteArray()));
					if (min != null)
					{
						var lastMinReplicatedEtag = min.ToEtag();
						prefetchingBehavior.CleanupDocuments(lastMinReplicatedEtag);
						prefetchingBehavior.UpdateAutoThrottler(documents, replicationDuration.Elapsed);
					}
				}
			}
		}

		private void SaveNewReplicationStatus(SqlReplicationStatus localReplicationStatus, Etag latestEtag)
		{
			int retries = 5;
			while (retries > 0)
			{
				retries--;
				try
				{
					var obj = RavenJObject.FromObject(localReplicationStatus);
					Database.Put(RavenSqlreplicationStatus, null, obj, new RavenJObject(), null);

					lastLatestEtag = latestEtag;
					break;
				}
				catch (ConcurrencyException)
				{
					Thread.Sleep(50);
				}
			}
		}

		private Etag HandleDeletesAndChangesMerging(List<ListItem> deletedDocs, List<JsonDocument> docsToReplicate)
		{
			// This code is O(N^2), I don't like it, but we don't have a lot of deletes, and in order for it to be really bad
			// we need a lot of deletes WITH a lot of changes at the same time
			for (int index = 0; index < deletedDocs.Count; index++)
			{
				var deletedDoc = deletedDocs[index];
				var change = docsToReplicate.FindIndex(
					x => string.Equals(x.Key, deletedDoc.Key, StringComparison.InvariantCultureIgnoreCase));

				if (change == -1)
					continue;

				// delete > doc
				if (deletedDoc.Etag.CompareTo(docsToReplicate[change].Etag) > 0)
				{
					// the delete came AFTER the doc, so we can remove the doc and just replicate the delete
					docsToReplicate.RemoveAt(change);
				}
				else
				{
					// the delete came BEFORE the doc, so we can remove the delte and just replicate the change
					deletedDocs.RemoveAt(index);
					index--;
				}
			}

			Etag latest = null;
			if (deletedDocs.Count != 0)
				latest = deletedDocs[deletedDocs.Count - 1].Etag;

			if (docsToReplicate.Count != 0)
			{
				var maybeLatest = docsToReplicate[docsToReplicate.Count - 1].Etag;
				Debug.Assert(maybeLatest != null);
				if (latest == null)
					return maybeLatest;
				if (maybeLatest.CompareTo(latest) > 0)
					return maybeLatest;
			}

			return latest;
		}

		private bool ReplicateDeletionsToDestination(SqlReplicationConfig cfg, IEnumerable<ListItem> deletedDocs)
		{
			var identifiers = deletedDocs.Select(x => x.Key).ToList();
			if (identifiers.Count == 0)
				return true;

			var replicationStats = statistics.GetOrAdd(cfg.Name, name => new SqlReplicationStatistics(name));
			using (var writer = new RelationalDatabaseWriter(Database, cfg, replicationStats))
			{
				foreach (var sqlReplicationTable in cfg.SqlReplicationTables)
				{
					writer.DeleteItems(sqlReplicationTable.TableName, sqlReplicationTable.DocumentKeyColumn, cfg.ParameterizeDeletesDisabled, identifiers);
				}
				writer.Commit();
			}

			return true;
		}

		private static string GetSqlReplicationDeletionName(SqlReplicationConfig replicationConfig)
		{
			return "SqlReplication/Deletions/" + replicationConfig.Name;
		}

		private Etag GetLeastReplicatedEtag(IEnumerable<SqlReplicationConfig> config, SqlReplicationStatus localReplicationStatus)
		{
			var synchronizationEtag = etagSynchronizer.GetSynchronizationEtag();
			Etag leastReplicatedEtag = null;
			foreach (var sqlReplicationConfig in config)
			{
				var lastEtag = GetLastEtagFor(localReplicationStatus, sqlReplicationConfig);
				if (leastReplicatedEtag == null)
					leastReplicatedEtag = lastEtag;
				else if (lastEtag.CompareTo(leastReplicatedEtag) < 0)
					leastReplicatedEtag = lastEtag;
			}
			var calculateSynchronizationEtag = etagSynchronizer.CalculateSynchronizationEtag(synchronizationEtag, leastReplicatedEtag);

			return calculateSynchronizationEtag;
		}

		private bool ReplicateChangesToDesintation(SqlReplicationConfig cfg, IEnumerable<JsonDocument> docs)
		{
			var scriptResult = ApplyConversionScript(cfg, docs);
			if (scriptResult.Data.Count == 0)
				return true;
			var replicationStats = statistics.GetOrAdd(cfg.Name, name => new SqlReplicationStatistics(name));
			var countOfItems = scriptResult.Data.Sum(x => x.Value.Count);
			try
			{
				using (var writer = new RelationalDatabaseWriter(Database, cfg, replicationStats))
				{
					if (writer.Execute(scriptResult))
						replicationStats.CompleteSuccess(countOfItems);
					else
						replicationStats.Success(countOfItems);
				}
				return true;
			}
			catch (Exception e)
			{
				log.WarnException("Failure to replicate changes to relational database for: " + cfg.Name, e);
				SqlReplicationStatistics replicationStatistics;
				DateTime newTime;
				if (statistics.TryGetValue(cfg.Name, out replicationStatistics) == false)
				{
					newTime = SystemTime.UtcNow.AddSeconds(5);
				}
				else
				{
					var totalSeconds = (SystemTime.UtcNow - replicationStatistics.LastErrorTime).TotalSeconds;
					newTime = SystemTime.UtcNow.AddSeconds(Math.Max(60 * 15, Math.Min(5, totalSeconds + 5)));
				}
				replicationStats.RecordWriteError(e, Database, countOfItems, newTime);
				return false;
			}
		}


		private ConversionScriptResult ApplyConversionScript(SqlReplicationConfig cfg, IEnumerable<JsonDocument> docs)
		{
			var replicationStats = statistics.GetOrAdd(cfg.Name, name => new SqlReplicationStatistics(name));
			var result = new ConversionScriptResult();
			foreach (var jsonDocument in docs)
			{
				Database.WorkContext.CancellationToken.ThrowIfCancellationRequested();
				if (string.IsNullOrEmpty(cfg.RavenEntityName) == false)
				{
					var entityName = jsonDocument.Metadata.Value<string>(Constants.RavenEntityName);
					if (string.Equals(cfg.RavenEntityName, entityName, StringComparison.InvariantCultureIgnoreCase) == false)
						continue;
				}
				var patcher = new SqlReplicationScriptedJsonPatcher(Database, result, cfg, jsonDocument.Key);
				try
				{
					DocumentRetriever.EnsureIdInMetadata(jsonDocument);
					var document = jsonDocument.ToJson();
					document[Constants.DocumentIdFieldName] = jsonDocument.Key;
					patcher.Apply(document, new ScriptedPatchRequest
					{
						Script = cfg.Script
					}, jsonDocument.SerializedSizeOnDisk);

					if (log.IsDebugEnabled && patcher.Debug.Count > 0)
					{
						log.Debug("Debug output for doc: {0} for script {1}:\r\n.{2}", jsonDocument.Key, cfg.Name, string.Join("\r\n", patcher.Debug));

						patcher.Debug.Clear();
					}

					replicationStats.ScriptSuccess();
				}
				catch (ParseException e)
				{
					replicationStats.MarkScriptAsInvalid(Database, cfg.Script);

					log.WarnException("Could parse SQL Replication script for " + cfg.Name, e);

					return result;
				}
				catch (Exception e)
				{
					replicationStats.RecordScriptError(Database);
					log.WarnException("Could not process SQL Replication script for " + cfg.Name + ", skipping document: " + jsonDocument.Key, e);
				}
			}
			return result;
		}


		private Etag GetLastEtagFor(SqlReplicationStatus replicationStatus, SqlReplicationConfig sqlReplicationConfig)
		{
			var lastEtag = Etag.Empty;
			var lastEtagHolder = replicationStatus.LastReplicatedEtags.FirstOrDefault(
				x => string.Equals(sqlReplicationConfig.Name, x.Name, StringComparison.InvariantCultureIgnoreCase));
			if (lastEtagHolder != null)
				lastEtag = lastEtagHolder.LastDocEtag;
			return lastEtag;
		}

		private List<SqlReplicationConfig> GetConfiguredReplicationDestinations()
		{
			var sqlReplicationConfigs = replicationConfigs;
			if (sqlReplicationConfigs != null)
				return sqlReplicationConfigs;

			sqlReplicationConfigs = new List<SqlReplicationConfig>();
			Database.TransactionalStorage.Batch(accessor =>
			{
				const string prefix = "Raven/SqlReplication/Configuration/";
				foreach (var document in accessor.Documents.GetDocumentsWithIdStartingWith(
								prefix, 0, 256))
				{
					var cfg = document.DataAsJson.JsonDeserialization<SqlReplicationConfig>();
					if (string.IsNullOrWhiteSpace(cfg.Name))
					{
						log.Warn("Could not find name for sql replication document {0}, ignoring", document.Key);
						continue;
					}
					if (string.IsNullOrWhiteSpace(cfg.ConnectionStringName) == false)
					{
						var connectionString = ConfigurationManager.ConnectionStrings[cfg.ConnectionStringName];
						if (connectionString == null)
						{
							log.Warn("Could not find connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
								cfg.ConnectionStringName,
								document.Key);
							continue;
						}
						cfg.ConnectionString = connectionString.ConnectionString;
					}
					else if (string.IsNullOrWhiteSpace(cfg.ConnectionStringSettingName) == false)
					{
						var setting = Database.Configuration.Settings[cfg.ConnectionStringSettingName];
						if (string.IsNullOrWhiteSpace(setting))
						{
							log.Warn("Could not find setting named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
								cfg.ConnectionStringName,
								document.Key);
							continue;
						}
					}
					sqlReplicationConfigs.Add(cfg);
				}
			});
			replicationConfigs = sqlReplicationConfigs;
			return sqlReplicationConfigs;
		}

		public void Dispose()
		{
			prefetchingBehavior.Dispose();
		}
	}
}