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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using metrics;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Config.Retriever;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Prefetching;
using Raven.Database.Storage;
using Raven.Json.Linq;

using Task = System.Threading.Tasks.Task;

namespace Raven.Database.Bundles.SqlReplication
{
	[InheritedExport(typeof(IStartupTask))]
	[ExportMetadata("Bundle", "sqlReplication")]
	[CLSCompliant(false)]
	public class SqlReplicationTask : IStartupTask, IDisposable
	{
		private const int MaxNumberOfDeletionsToReplicate = 1024;

		private volatile bool shouldPause;

		public bool IsRunning { get; private set; }

		private class ReplicatedDoc
		{
			public RavenJObject Document;
			public Etag Etag;
			public int SerializedSizeOnDisk;
			public string Key;
		}

		public const string RavenSqlReplicationStatus = "Raven/SqlReplication/Status";

		private readonly static ILog log = LogManager.GetCurrentClassLogger();

		public event Action<int> AfterReplicationCompleted = delegate { };
		readonly Metrics sqlReplicationMetrics = new Metrics();

		public DocumentDatabase Database { get; set; }

		private List<SqlReplicationConfig> replicationConfigs;
		private readonly ConcurrentDictionary<string, SqlReplicationStatistics> statistics = new ConcurrentDictionary<string, SqlReplicationStatistics>(StringComparer.InvariantCultureIgnoreCase);
		public ConcurrentDictionary<string, SqlReplicationStatistics> Statistics
		{
			get { return statistics; }
		}
		private PrefetchingBehavior prefetchingBehavior;

		public readonly ConcurrentDictionary<string, SqlReplicationMetricsCountersManager> SqlReplicationMetricsCounters =
			new ConcurrentDictionary<string, SqlReplicationMetricsCountersManager>();

		public void Execute(DocumentDatabase database)
		{
			prefetchingBehavior = database.Prefetcher.CreatePrefetchingBehavior(PrefetchingUser.SqlReplicator, null);

			Database = database;
			Database.Notifications.OnDocumentChange += (sender, notification, metadata) =>
			{
				if (notification.Id == null)
					return;

				if (metadata == null)
					return; // this is a delete being made on an already deleted document

				if (notification.Type == DocumentChangeTypes.Delete)
				{
					RecordDelete(notification.Id, metadata);
				}

				if (!notification.Id.StartsWith("Raven/SqlReplication/Configuration/", StringComparison.InvariantCultureIgnoreCase)
					&& string.Compare(notification.Id, "Raven/SqlReplication/Connections", StringComparison.InvariantCultureIgnoreCase) != 0)
					return;

				replicationConfigs = null;
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

		public void Pause()
		{
			shouldPause = true;
		}

		public void Continue()
		{
			shouldPause = false;
		}

		private void RecordDelete(string id, RavenJObject metadata)
		{
			Database.TransactionalStorage.Batch(accessor =>
			{
				bool hasChanges = false;
				foreach (var config in GetConfiguredReplicationDestinations())
				{
					if (string.Equals(config.RavenEntityName, metadata.Value<string>(Constants.RavenEntityName), StringComparison.InvariantCultureIgnoreCase) == false)
						continue;

					hasChanges = true;
					accessor.Lists.Set(GetSqlReplicationDeletionName(config), id, metadata, UuidType.Documents);
				}
				if (hasChanges)
					Database.WorkContext.ShouldNotifyAboutWork(() => "recorded a deleted document " + id);
			});
			if (log.IsDebugEnabled)
				log.Debug(() => "recorded a deleted document " + id);
		}

		private SqlReplicationStatus GetReplicationStatus()
		{
			var jsonDocument = Database.Documents.Get(RavenSqlReplicationStatus, null);
			return jsonDocument == null
									? new SqlReplicationStatus()
									: jsonDocument.DataAsJson.JsonDeserialization<SqlReplicationStatus>();
		}

		public SqlReplicationMetricsCountersManager GetSqlReplicationMetricsManager(SqlReplicationConfig cfg)
		{
			return SqlReplicationMetricsCounters.GetOrAdd(cfg.Name,
				s => new SqlReplicationMetricsCountersManager(sqlReplicationMetrics, cfg)
				);
		}

		private void BackgroundSqlReplication()
		{
			int workCounter = 0;
			while (Database.WorkContext.DoWork)
			{
				IsRunning = !shouldPause;

				if (!IsRunning)
					continue;

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
					return SystemTime.UtcNow >= sqlReplicationStatistics.SuspendUntil;
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

				List<JsonDocument> documents;

				using (prefetchingBehavior.DocumentBatchFrom(leastReplicatedEtag, out documents))
				{
					Etag latestEtag = null, lastBatchEtag = null;
					if (documents.Count != 0)
						lastBatchEtag = documents[documents.Count - 1].Etag;

					var replicationDuration = Stopwatch.StartNew();
					documents.RemoveAll(x => x.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase)); // we ignore system documents here

					if (documents.Count != 0)
						latestEtag = documents[documents.Count - 1].Etag;

					documents.RemoveAll(x => prefetchingBehavior.FilterDocuments(x) == false);

					var deletedDocsByConfig = new Dictionary<SqlReplicationConfig, List<ListItem>>();

					foreach (var relevantConfig in relevantConfigs)
					{
						var cfg = relevantConfig;
						Database.TransactionalStorage.Batch(accessor =>
						{
							deletedDocsByConfig[cfg] = accessor.Lists.Read(GetSqlReplicationDeletionName(cfg),
															  GetLastEtagFor(localReplicationStatus, cfg),
															  latestEtag,
															  MaxNumberOfDeletionsToReplicate + 1)
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
							SaveNewReplicationStatus(localReplicationStatus);
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
						var itemsToReplicate = documents.Select(x =>
						{
							JsonDocument.EnsureIdInMetadata(x);
							var doc = x.ToJson();
							doc[Constants.DocumentIdFieldName] = x.Key;

							return new ReplicatedDoc
							{
								Document = doc,
								Etag = x.Etag,
								Key = x.Key,
								SerializedSizeOnDisk = x.SerializedSizeOnDisk
							};
						}).ToList();

						BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(Database.WorkContext, relevantConfigs, replicationConfig =>
						{
							try
							{
								var startTime = SystemTime.UtcNow;
								Stopwatch spRepTime = new Stopwatch();
								spRepTime.Start();
								var lastReplicatedEtag = GetLastEtagFor(localReplicationStatus, replicationConfig);

								var deletedDocs = deletedDocsByConfig[replicationConfig];
								var docsToReplicate = itemsToReplicate
									.Where(x => lastReplicatedEtag.CompareTo(x.Etag) < 0) // haven't replicate the etag yet
									.Where(document =>
									{
										var info = Database.Documents.GetRecentTouchesFor(document.Key);
										if (info != null)
										{
											if (info.TouchedEtag.CompareTo(lastReplicatedEtag) > 0)
											{
												log.Debug(
													"Will not replicate document '{0}' to '{1}' because the updates after etag {2} are related document touches",
													document.Key, replicationConfig.Name, info.TouchedEtag);
												return false;
											}
										}
										return true;
									});

								if (deletedDocs.Count >= MaxNumberOfDeletionsToReplicate + 1)
									docsToReplicate = docsToReplicate.Where(x => EtagUtil.IsGreaterThan(x.Etag, deletedDocs[deletedDocs.Count - 1].Etag) == false);

								var docsToReplicateAsList = docsToReplicate.ToList();

								var currentLatestEtag = HandleDeletesAndChangesMerging(deletedDocs, docsToReplicateAsList);
								if (currentLatestEtag == null && itemsToReplicate.Count > 0 && docsToReplicateAsList.Count == 0)
									currentLatestEtag = lastBatchEtag;

								int countOfReplicatedItems = 0;
								if (ReplicateDeletionsToDestination(replicationConfig, deletedDocs) &&
																					ReplicateChangesToDestination(replicationConfig, docsToReplicateAsList, out countOfReplicatedItems))
								{
									if (deletedDocs.Count > 0)
									{
										Database.TransactionalStorage.Batch(accessor =>
											accessor.Lists.RemoveAllBefore(GetSqlReplicationDeletionName(replicationConfig), deletedDocs[deletedDocs.Count - 1].Etag));
									}
									successes.Enqueue(Tuple.Create(replicationConfig, currentLatestEtag));
								}

								spRepTime.Stop();
								var elapsedMicroseconds = (long)(spRepTime.ElapsedTicks * SystemTime.MicroSecPerTick);

								var sqlReplicationMetricsCounters = GetSqlReplicationMetricsManager(replicationConfig);
								sqlReplicationMetricsCounters.SqlReplicationBatchSizeMeter.Mark(countOfReplicatedItems);
								sqlReplicationMetricsCounters.SqlReplicationBatchSizeHistogram.Update(countOfReplicatedItems);
								sqlReplicationMetricsCounters.SqlReplicationDurationHistogram.Update(elapsedMicroseconds);

								UpdateReplicationPerformance(replicationConfig, startTime, spRepTime.Elapsed, docsToReplicateAsList.Count);

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
								var lastDocEtag = destEtag.LastDocEtag;
								if (currentLatestEtag != null && EtagUtil.IsGreaterThan(currentLatestEtag, lastDocEtag))
									lastDocEtag = currentLatestEtag;

								destEtag.LastDocEtag = lastDocEtag;
							}
						}

						SaveNewReplicationStatus(localReplicationStatus);
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
		}

		private void UpdateReplicationPerformance(SqlReplicationConfig replicationConfig, DateTime startTime, TimeSpan elapsed, int batchSize)
		{
			var performance = new SqlReplicationPerformanceStats
			{
				BatchSize = batchSize,
				Duration = elapsed,
				Started = startTime
			};

			var sqlReplicationMetricsCounters = GetSqlReplicationMetricsManager(replicationConfig);
			sqlReplicationMetricsCounters.ReplicationPerformanceStats.Enqueue(performance);
			while (sqlReplicationMetricsCounters.ReplicationPerformanceStats.Count() > 25)
			{
				SqlReplicationPerformanceStats _;
				sqlReplicationMetricsCounters.ReplicationPerformanceStats.TryDequeue(out _);
			}
		}

		private void SaveNewReplicationStatus(SqlReplicationStatus localReplicationStatus)
		{
			int retries = 5;
			while (retries > 0)
			{
				retries--;
				try
				{
					var obj = RavenJObject.FromObject(localReplicationStatus);
					Database.Documents.Put(RavenSqlReplicationStatus, null, obj, new RavenJObject(), null);

					break;
				}
				catch (ConcurrencyException)
				{
					Thread.Sleep(50);
				}
			}
		}

		private Etag HandleDeletesAndChangesMerging(List<ListItem> deletedDocs, List<ReplicatedDoc> docsToReplicate)
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
				log.Debug("Replicated deletes of {0} for config {1}", string.Join(", ", identifiers), cfg.Name);
			}

			return true;
		}

		private static string GetSqlReplicationDeletionName(SqlReplicationConfig replicationConfig)
		{
			return "SqlReplication/Deletions/" + replicationConfig.Name;
		}

		private Etag GetLeastReplicatedEtag(IEnumerable<SqlReplicationConfig> config, SqlReplicationStatus localReplicationStatus)
		{
			Etag leastReplicatedEtag = null;
			foreach (var sqlReplicationConfig in config)
			{
				var lastEtag = GetLastEtagFor(localReplicationStatus, sqlReplicationConfig);
				if (leastReplicatedEtag == null)
					leastReplicatedEtag = lastEtag;
				else if (lastEtag.CompareTo(leastReplicatedEtag) < 0)
					leastReplicatedEtag = lastEtag;
			}

			return leastReplicatedEtag;
		}

		private bool ReplicateChangesToDestination(SqlReplicationConfig cfg, IEnumerable<ReplicatedDoc> docs, out int countOfReplicatedItems)
		{
			countOfReplicatedItems = 0;
            var replicationStats = statistics.GetOrAdd(cfg.Name, name => new SqlReplicationStatistics(name));
			var scriptResult = ApplyConversionScript(cfg, docs, replicationStats);
			if (scriptResult.Data.Count == 0)
				return true;
			
			countOfReplicatedItems = scriptResult.Data.Sum(x => x.Value.Count);
			try
			{
				using (var writer = new RelationalDatabaseWriter(Database, cfg, replicationStats))
				{
					if (writer.Execute(scriptResult))
					{
						log.Debug("Replicated changes of {0} for replication {1}", string.Join(", ", docs.Select(d => d.Key)), cfg.Name);
						replicationStats.CompleteSuccess(countOfReplicatedItems);
					}
					else
					{
						log.Debug("Replicated changes (with some errors) of {0} for replication {1}", string.Join(", ", docs.Select(d => d.Key)), cfg.Name);
						replicationStats.Success(countOfReplicatedItems);
					}
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
                    if (replicationStatistics.LastErrorTime == DateTime.MinValue)
                    {
                        newTime = SystemTime.UtcNow.AddSeconds(5);
                    }
                    else
                    {
                        // double the fallback time (but don't cross 15 minutes)
                        var totalSeconds = (SystemTime.UtcNow - replicationStatistics.LastErrorTime).TotalSeconds;
                        newTime = SystemTime.UtcNow.AddSeconds(Math.Min(60*15, Math.Max(5, totalSeconds*2)));
                    }
				}
				replicationStats.RecordWriteError(e, Database, countOfReplicatedItems, newTime);
				return false;
			}
		}

		private ConversionScriptResult ApplyConversionScript(SqlReplicationConfig cfg, IEnumerable<ReplicatedDoc> docs, SqlReplicationStatistics replicationStats)
		{
			var result = new ConversionScriptResult();
			foreach (var replicatedDoc in docs)
			{
				Database.WorkContext.CancellationToken.ThrowIfCancellationRequested();
				if (string.IsNullOrEmpty(cfg.RavenEntityName) == false)
				{
					var entityName = replicatedDoc.Document[Constants.Metadata].Value<string>(Constants.RavenEntityName);
					if (string.Equals(cfg.RavenEntityName, entityName, StringComparison.InvariantCultureIgnoreCase) == false)
						continue;
				}

				var patcher = new SqlReplicationScriptedJsonPatcher(Database, result, cfg, replicatedDoc.Key);
				using (var scope = new SqlReplicationScriptedJsonPatcherOperationScope(Database))
				{
					try
					{
						patcher.Apply(scope, replicatedDoc.Document, new ScriptedPatchRequest { Script = cfg.Script }, replicatedDoc.SerializedSizeOnDisk);

						if (log.IsDebugEnabled && patcher.Debug.Count > 0)
						{
							log.Debug("Debug output for doc: {0} for script {1}:\r\n.{2}", replicatedDoc.Key, cfg.Name, string.Join("\r\n", patcher.Debug));

							patcher.Debug.Clear();
						}

						replicationStats.ScriptSuccess();
					}
					catch (ParseException e)
					{
						replicationStats.MarkScriptAsInvalid(Database, cfg.Script);

						log.WarnException("Could not parse SQL Replication script for " + cfg.Name, e);

						return result;
					}
					catch (Exception diffExceptionName)
					{
						replicationStats.RecordScriptError(Database, diffExceptionName);
						log.WarnException("Could not process SQL Replication script for " + cfg.Name + ", skipping document: " + replicatedDoc.Key, diffExceptionName);
					}
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

		public RelationalDatabaseWriter.TableQuerySummary[] SimulateSqlReplicationSQLQueries(string strDocumentId, SqlReplicationConfig sqlReplication, bool performRolledbackTransaction, out Alert alert)
		{
			alert = null;
			RelationalDatabaseWriter.TableQuerySummary[] resutls = null;

			try
			{
				var stats = new SqlReplicationStatistics(sqlReplication.Name, false);

				var jsonDocument = Database.Documents.Get(strDocumentId, null);
				JsonDocument.EnsureIdInMetadata(jsonDocument);
				var doc = jsonDocument.ToJson();
				doc[Constants.DocumentIdFieldName] = jsonDocument.Key;

				var docs = new List<ReplicatedDoc>
                           {
	                           new ReplicatedDoc
	                           {
		                           Document = doc,
								   Etag = jsonDocument.Etag,
								   Key = jsonDocument.Key,
								   SerializedSizeOnDisk = jsonDocument.SerializedSizeOnDisk
	                           }
                           };
				var scriptResult = ApplyConversionScript(sqlReplication, docs, stats);

				var sqlReplicationConnections = Database.ConfigurationRetriever.GetConfigurationDocument<SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin>>(Constants.SqlReplication.SqlReplicationConnectionsDocumentName);

				if (PrepareSqlReplicationConfig(sqlReplication, sqlReplication.Name, stats, sqlReplicationConnections.MergedDocument, false, false))
				{
					if (performRolledbackTransaction)
					{
						using (var writer = new RelationalDatabaseWriter(Database, sqlReplication, stats))
						{
							resutls = writer.RolledBackExecute(scriptResult).ToArray();
						}
					}
					else
					{
						var simulatedwriter = new RelationalDatabaseWriterSimulator(Database, sqlReplication, stats);
						resutls = new List<RelationalDatabaseWriter.TableQuerySummary>()
                        {
                            new RelationalDatabaseWriter.TableQuerySummary()
                            {
                                Commands = simulatedwriter.SimulateExecuteCommandText(scriptResult)
                                    .Select(x => new RelationalDatabaseWriter.TableQuerySummary.CommandData()
                                    {
                                        CommandText = x
                                    }).ToArray()
                            }
                        }.ToArray();


					}
				}

				alert = stats.LastAlert;
			}
			catch (Exception e)
			{
				alert = new Alert()
				{
					AlertLevel = AlertLevel.Error,
					CreatedAt = SystemTime.UtcNow,
					Message = "Last SQL replication operation for " + sqlReplication.Name + " was failed",
					Title = "SQL replication error",
					Exception = e.ToString(),
					UniqueKey = "Sql Replication Error: " + sqlReplication.Name
				};
			}
			return resutls;
		}

		public List<SqlReplicationConfig> GetConfiguredReplicationDestinations()
		{
			var sqlReplicationConfigs = replicationConfigs;
			if (sqlReplicationConfigs != null)
				return sqlReplicationConfigs;

			sqlReplicationConfigs = new List<SqlReplicationConfig>();
			Database.TransactionalStorage.Batch(accessor =>
			{
				const string prefix = "Raven/SqlReplication/Configuration/";

				var configurationDocument = Database.ConfigurationRetriever.GetConfigurationDocument<SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin>>(Constants.SqlReplication.SqlReplicationConnectionsDocumentName);
				var sqlReplicationConnections = configurationDocument != null ? configurationDocument.MergedDocument : new SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin>(); // backward compatibility

				foreach (var sqlReplicationConfigDocument in accessor.Documents.GetDocumentsWithIdStartingWith(prefix, 0, int.MaxValue, null))
				{
					var cfg = sqlReplicationConfigDocument.DataAsJson.JsonDeserialization<SqlReplicationConfig>();
					var replicationStats = statistics.GetOrAdd(cfg.Name, name => new SqlReplicationStatistics(name));
					if (!PrepareSqlReplicationConfig(cfg, sqlReplicationConfigDocument.Key, replicationStats, sqlReplicationConnections)) continue;
					sqlReplicationConfigs.Add(cfg);
				}
			});
			replicationConfigs = sqlReplicationConfigs;
			return sqlReplicationConfigs;
		}

		private bool PrepareSqlReplicationConfig(SqlReplicationConfig cfg, string sqlReplicationConfigDocumentKey, SqlReplicationStatistics replicationStats, SqlReplicationConnections<SqlReplicationConnections.PredefinedSqlConnectionWithConfigurationOrigin> sqlReplicationConnections, bool writeToLog = true, bool validateSqlReplicationName = true)
		{
			if (validateSqlReplicationName && string.IsNullOrWhiteSpace(cfg.Name))
			{
				if (writeToLog)
					log.Warn("Could not find name for sql replication document {0}, ignoring", sqlReplicationConfigDocumentKey);
				replicationStats.LastAlert = new Alert()
				{
					AlertLevel = AlertLevel.Error,
					CreatedAt = DateTime.UtcNow,
					Title = "Could not start replication",
					Message = string.Format("Could not find name for sql replication document {0}, ignoring", sqlReplicationConfigDocumentKey)
				};
				return false;
			}
			if (string.IsNullOrWhiteSpace(cfg.PredefinedConnectionStringSettingName) == false)
			{
				var matchingConnection = sqlReplicationConnections.PredefinedConnections.FirstOrDefault(x => string.Compare(x.Name, cfg.PredefinedConnectionStringSettingName, StringComparison.InvariantCultureIgnoreCase) == 0);
				if (matchingConnection != null)
				{
					cfg.ConnectionString = matchingConnection.ConnectionString;
					cfg.FactoryName = matchingConnection.FactoryName;
				}
				else
				{
					if (writeToLog)
						log.Warn("Could not find predefined connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
							cfg.PredefinedConnectionStringSettingName,
							sqlReplicationConfigDocumentKey);
					replicationStats.LastAlert = new Alert()
					{
						AlertLevel = AlertLevel.Error,
						CreatedAt = DateTime.UtcNow,
						Title = "Could not start replication",
						Message = string.Format("Could not find predefined connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
							cfg.PredefinedConnectionStringSettingName,
							sqlReplicationConfigDocumentKey)
					};
					return false;
				}
			}
			else if (string.IsNullOrWhiteSpace(cfg.ConnectionStringName) == false)
			{
				var connectionString = ConfigurationManager.ConnectionStrings[cfg.ConnectionStringName];
				if (connectionString == null)
				{
					if (writeToLog)
						log.Warn("Could not find connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
							cfg.ConnectionStringName, sqlReplicationConfigDocumentKey);

					replicationStats.LastAlert = new Alert()
					{
						AlertLevel = AlertLevel.Error,
						CreatedAt = DateTime.UtcNow,
						Title = "Could not start replication",
						Message = string.Format("Could not find connection string named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
							cfg.ConnectionStringName,
							sqlReplicationConfigDocumentKey)
					};
					return false;
				}
				cfg.ConnectionString = connectionString.ConnectionString;
			}
			else if (string.IsNullOrWhiteSpace(cfg.ConnectionStringSettingName) == false)
			{
				var setting = Database.Configuration.Settings[cfg.ConnectionStringSettingName];
				if (string.IsNullOrWhiteSpace(setting))
				{
					if (writeToLog)
						log.Warn("Could not find setting named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
			cfg.ConnectionStringSettingName,
			sqlReplicationConfigDocumentKey);
					replicationStats.LastAlert = new Alert()
					{
						AlertLevel = AlertLevel.Error,
						CreatedAt = DateTime.UtcNow,
						Title = "Could not start replication",
						Message = string.Format("Could not find setting named '{0}' for sql replication config: {1}, ignoring sql replication setting.",
							cfg.ConnectionStringSettingName,
							sqlReplicationConfigDocumentKey)
					};
					return false;
				}
			}
			return true;
		}

		public void Dispose()
		{
			if (prefetchingBehavior != null)
				prefetchingBehavior.Dispose();
		}
	}
}