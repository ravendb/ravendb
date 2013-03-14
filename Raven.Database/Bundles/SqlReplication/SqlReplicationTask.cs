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
using System.Data.Common;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Impl;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Json.Linq;
using Task = System.Threading.Tasks.Task;
using System.Linq;

namespace Raven.Database.Bundles.SqlReplication
{
	[InheritedExport(typeof(IStartupTask))]
	[ExportMetadata("Bundle", "sqlReplication")]
	public class SqlReplicationTask : IStartupTask
	{
		private const string RavenSqlreplicationStatus = "Raven/SqlReplication/Status";
		private readonly static ILog log = LogManager.GetCurrentClassLogger();

		public event Action AfterReplicationCompleted = delegate { };

		public DocumentDatabase Database { get; set; }

		private List<SqlReplicationConfig> replicationConfigs;
		private readonly ConcurrentDictionary<string, SqlReplicationStatistics> statistics = new ConcurrentDictionary<string, SqlReplicationStatistics>(StringComparer.InvariantCultureIgnoreCase);

		private PrefetchingBehavior prefetchingBehavior;

		public void Execute(DocumentDatabase database)
		{
			Database = database;
			Database.OnDocumentChange += (sender, notification) =>
			{
				if (notification.Id == null)
					return;
				if (!notification.Id.StartsWith("Raven/SqlReplication/Configuration/", StringComparison.InvariantCultureIgnoreCase))
					return;

				replicationConfigs = null;
				statistics.Clear();
			};

			GetReplicationStatus();

			prefetchingBehavior = new PrefetchingBehavior(Database.WorkContext, new IndexBatchSizeAutoTuner(Database.WorkContext));

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
				var leastReplicatedEtag = GetLeastReplicatedEtag(config, localReplicationStatus);

				if (leastReplicatedEtag == null)
				{
					Database.WorkContext.WaitForWork(TimeSpan.FromMinutes(10), ref workCounter, "Sql Replication");
					continue;
				}

				var documents = prefetchingBehavior.GetDocumentsBatchFrom(leastReplicatedEtag.Value);
				if (documents.Count == 0 ||
					documents.All(x => x.Key.StartsWith("Raven/", StringComparison.InvariantCultureIgnoreCase))) // ignore changes for system docs
				{
					Database.WorkContext.WaitForWork(TimeSpan.FromMinutes(10), ref workCounter, "Sql Replication");
					continue;
				}

				var latestEtag = documents.Last().Etag.Value;

				var relevantConfigs =
					config
						.Where(x => ByteArrayComparer.Instance.Compare(GetLastEtagFor(localReplicationStatus, x), latestEtag) <= 0) // haven't replicate the etag yet
						.Where(x =>
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

				try
				{
					var successes = new ConcurrentQueue<SqlReplicationConfig>();
					BackgroundTaskExecuter.Instance.ExecuteAllInterleaved(Database.WorkContext, relevantConfigs, replicationConfig =>
					{
						try
						{
							if (ReplicateToDesintation(replicationConfig, documents))
							{
								successes.Enqueue(replicationConfig);
							}
						}
						catch (Exception e)
						{
							log.WarnException("Error while replication to SQL destination: " + replicationConfig.Name, e);
						}
					});
					if (successes.Count == 0)
						continue;
					foreach (var cfg in successes)
					{
						var destEtag = localReplicationStatus.LastReplicatedEtags.FirstOrDefault(x => string.Equals(x.Name, cfg.Name, StringComparison.InvariantCultureIgnoreCase));
						if (destEtag == null)
						{
							localReplicationStatus.LastReplicatedEtags.Add(new LastReplicatedEtag
							{
								Name = cfg.Name,
								LastDocEtag = latestEtag
							});
						}
						else
						{
							destEtag.LastDocEtag = latestEtag;
						}
					}

					var obj = RavenJObject.FromObject(localReplicationStatus);
					Database.Put(RavenSqlreplicationStatus, null, obj, new RavenJObject(), null);
				}
				finally
				{
					AfterReplicationCompleted();
				}
			}
		}

		private Guid? GetLeastReplicatedEtag(List<SqlReplicationConfig> config, SqlReplicationStatus localReplicationStatus)
		{
			Guid? leastReplicatedEtag = null;
			foreach (var sqlReplicationConfig in config)
			{
				var lastEtag = GetLastEtagFor(localReplicationStatus, sqlReplicationConfig);
				if (leastReplicatedEtag == null)
					leastReplicatedEtag = lastEtag;
				else if (ByteArrayComparer.Instance.Compare(lastEtag, leastReplicatedEtag.Value) < 0)
					leastReplicatedEtag = lastEtag;
			}
			return leastReplicatedEtag;
		}

		private bool ReplicateToDesintation(SqlReplicationConfig cfg, IEnumerable<JsonDocument> docs)
		{
			var providerFactory = TryGetDbProviderFactory(cfg);
			if (providerFactory == null)
				return false;

			var scriptResult = ApplyConversionScript(cfg, docs);
			if (scriptResult.Data.Count == 0)
				return true;
			var replicationStats = statistics.GetOrAdd(cfg.Name, name => new SqlReplicationStatistics(name));
			var countOfItems = scriptResult.Data.Sum(x => x.Value.Count);
			try
			{
				if (WriteToRelationalDatabase(cfg, providerFactory, scriptResult, replicationStats))
					replicationStats.CompleteSuccess(countOfItems);
				else
					replicationStats.Success(countOfItems);

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
				if (string.IsNullOrEmpty(cfg.RavenEntityName) == false)
				{
					var entityName = jsonDocument.Metadata.Value<string>(Constants.RavenEntityName);
					if (string.Equals(cfg.RavenEntityName, entityName, StringComparison.InvariantCultureIgnoreCase) == false)
						continue;
				}
				var patcher = new SqlReplicationScriptedJsonPatcher(Database, result, jsonDocument.Key);
				try
				{
					DocumentRetriever.EnsureIdInMetadata(jsonDocument);
					jsonDocument.Metadata[Constants.DocumentIdFieldName] = jsonDocument.Key;
					var document = jsonDocument.ToJson();
					patcher.Apply(document, new ScriptedPatchRequest
					{
						Script = cfg.Script
					});

					replicationStats.ScriptSuccess();
				}
				catch (ParseException e)
				{
					replicationStats.MarkScriptAsInvalid(Database, cfg.Script);

					log.WarnException("Could parse SQL Replication script for " + cfg.Name, e);

					return dictionary;
				}
				catch (Exception e)
				{
					replicationStats.RecordScriptError(Database);
					log.WarnException("Could not process SQL Replication script for " + cfg.Name + ", skipping this document", e);
				}
			}
			return dictionary;
		}

		private DbProviderFactory TryGetDbProviderFactory(SqlReplicationConfig cfg)
		{
			DbProviderFactory providerFactory;
			try
			{
				providerFactory = DbProviderFactories.GetFactory(cfg.FactoryName);
			}
			catch (Exception e)
			{
				log.WarnException(
					string.Format("Could not find provider factory {0} to replicate to sql for {1}, ignoring", cfg.FactoryName,
								  cfg.Name), e);

				Database.AddAlert(new Alert
				{
					AlertLevel = AlertLevel.Error,
					CreatedAt = SystemTime.UtcNow,
					Exception = e.ToString(),
					Title = "Sql Replication Count not find factory provider",
					Message = string.Format("Could not find factory provider {0} to replicate to sql for {1}, ignoring", cfg.FactoryName,
								  cfg.Name),
					UniqueKey = string.Format("Sql Replication Provider Not Found: {0}, {1}", cfg.Name, cfg.FactoryName)
				});

				return null;
			}
			return providerFactory;
		}
	
	
		private Guid GetLastEtagFor(SqlReplicationStatus replicationStatus, SqlReplicationConfig sqlReplicationConfig)
		{
			var lastEtag = Guid.Empty;
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
	}
}