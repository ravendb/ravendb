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
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Jint;
using Jint.Native;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Plugins;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Task = System.Threading.Tasks.Task;
using System.Linq;

namespace Raven.Database.Bundles.SqlReplication
{
	[InheritedExport(typeof(IStartupTask))]
	[ExportMetadata("Bundle", "sqlReplication")]
	public class SqlReplicationTask : IStartupTask
	{
		private readonly static ILog log = LogManager.GetCurrentClassLogger();

		public event Action AfterReplicationCompleted = delegate { };

		public DocumentDatabase Database { get; set; }

		private List<SqlReplicationConfig> replicationConfigs;
		private ConcurrentDictionary<string, DateTime> lastError = new ConcurrentDictionary<string, DateTime>(StringComparer.InvariantCultureIgnoreCase);

		private SqlReplicationStatus replicationStatus;
		private PrefetchingBehavior prefetchingBehavior;

		public void Execute(DocumentDatabase database)
		{
			Database = database;
			Database.OnDocumentChange += (sender, notification) =>
			{
				if (notification.Id == null ||
					notification.Id.StartsWith("Raven/SqlReplication/Configuration/", StringComparison.InvariantCultureIgnoreCase) == false)
					return;
				replicationConfigs = null;
			};

			var jsonDocument = Database.Get("Raven/SqlReplication/Status", null);
			replicationStatus = jsonDocument == null ? new SqlReplicationStatus() : jsonDocument.DataAsJson.JsonDeserialization<SqlReplicationStatus>();

			prefetchingBehavior = new PrefetchingBehavior(Database.WorkContext, new IndexBatchSizeAutoTuner(Database.WorkContext));

			var task = Task.Factory.StartNew(() =>
			{
				try
				{
					BackgroundSqlReplication();
				}
				catch (Exception e)
				{
					log.ErrorException("Fatal failure when replicating to SQL. All SQL Replication activity STOPPED", e);
				}
			}, TaskCreationOptions.LongRunning);
			database.ExtensionsState.GetOrAdd(typeof(SqlReplicationTask).FullName, k => new DisposableAction(task.Wait));
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
				var leastReplicatedEtag = Guid.Empty;
				foreach (var sqlReplicationConfig in config)
				{
					var lastEtag = GetLastEtagFor(sqlReplicationConfig);
					if (ByteArrayComparer.Instance.Compare(lastEtag, leastReplicatedEtag) < 0)
						leastReplicatedEtag = lastEtag;
				}

				var relevantConfigs =
					config
						.Where(x => ByteArrayComparer.Instance.Compare(GetLastEtagFor(x), leastReplicatedEtag) <= 0) // haven't replicate the etag yet
						.Where(x => SystemTime.UtcNow >= lastError.GetOrDefault(x.Name)) // have error or the timeout expired
						.ToList();

				var documents = prefetchingBehavior.GetDocumentsBatchFrom(leastReplicatedEtag);
				if (documents.Count == 0)
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
								successes.Enqueue(replicationConfig);
						}
						catch (Exception e)
						{
							log.WarnException("Error while replication to SQL destination: " + replicationConfig.Name, e);
						}
					});

					var lastReplicatedEtag = documents.Last().Etag.Value;
					foreach (var cfg in successes)
					{
						var destEtag = replicationStatus.LastReplicatedEtags.FirstOrDefault(x => string.Equals(x.Name, cfg.Name, StringComparison.InvariantCultureIgnoreCase));
						if (destEtag == null)
						{
							replicationStatus.LastReplicatedEtags.Add(new LastReplicatedEtag
							{
								Name = cfg.Name,
								LastDocEtag = lastReplicatedEtag
							});
						}
						else
						{
							destEtag.LastDocEtag = lastReplicatedEtag;
						}
					}

					Database.Put("Raven/SqlReplication/Status", null, RavenJObject.FromObject(replicationStatus), new RavenJObject(),
								 null);
				}
				finally
				{
					AfterReplicationCompleted();
				}
			}
		}

		private bool ReplicateToDesintation(SqlReplicationConfig cfg, IEnumerable<JsonDocument> docs)
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

				lastError[cfg.Name] = DateTime.MaxValue; // always error 

				return false;
			}

			var dictionary = new Dictionary<string, List<ItemToReplicate>>();
			foreach (var jsonDocument in docs)
			{
				if (string.IsNullOrEmpty(cfg.RavenEntityName) == false)
				{
					var entityName = jsonDocument.Metadata.Value<string>(Constants.RavenEntityName);
					if (string.Equals(cfg.RavenEntityName, entityName, StringComparison.InvariantCultureIgnoreCase) == false)
						continue;
				}
				var patcher = new SqlReplicationScriptedJsonPatcher(Database, dictionary, jsonDocument.Key);
				try
				{
					patcher.Apply(jsonDocument.ToJson(), new ScriptedPatchRequest
					{
						Script = cfg.Script
					});
				}
				catch (Exception e)
				{
					log.WarnException("Could not process SQL Replication script for " + cfg.Name +", skipping this document", e);
				}
			}
			try
			{
				WriteToRelationalDatabase(cfg, providerFactory, dictionary);
				return true;
			}
			catch (Exception e)
			{
				log.WarnException("Failure to replicate changes to relational database for " + cfg.Name +", updates", e);
				DateTime time;
					DateTime newTime;
				if (lastError.TryGetValue(cfg.Name, out time) == false)
				{
					newTime = SystemTime.UtcNow.AddMinutes(1);
				}
				else
				{
					var totalMinutes = (SystemTime.UtcNow - time).TotalMinutes;
					newTime = SystemTime.UtcNow.AddMinutes(Math.Max(10, Math.Min(1, totalMinutes + 1)));
				}
				lastError[cfg.Name] = newTime;
				return false;
			}
		}

		private void WriteToRelationalDatabase(SqlReplicationConfig cfg, DbProviderFactory providerFactory,
		                                       Dictionary<string, List<ItemToReplicate>> dictionary)
		{
			using (var commandBuilder = providerFactory.CreateCommandBuilder())
			using (var connection = providerFactory.CreateConnection())
			{
				Debug.Assert(connection != null);
				Debug.Assert(commandBuilder != null);
				connection.ConnectionString = cfg.ConnectionString;
				connection.Open();
				using (var tx = connection.BeginTransaction())
				{
					foreach (var kvp in dictionary)
					{
						// first, delete all the rows that might already exist there
						foreach (var itemToReplicate in kvp.Value)
						{
							using (var cmd = connection.CreateCommand())
							{
								cmd.Transaction = tx;
								var dbParameter = cmd.CreateParameter();
								dbParameter.ParameterName = GetParameterName(providerFactory, commandBuilder, itemToReplicate.PkName);
								cmd.Parameters.Add(dbParameter);
								dbParameter.Value = itemToReplicate.Columns.Value<object>(itemToReplicate.PkName);
								cmd.CommandText = string.Format("DELETE FROM {0} WHERE {1} = {2}",
								                                commandBuilder.QuoteIdentifier(kvp.Key),
								                                commandBuilder.QuoteIdentifier(itemToReplicate.PkName),
								                                dbParameter.ParameterName
									);
								cmd.ExecuteNonQuery();
							}
						}

						foreach (var itemToReplicate in kvp.Value)
						{
							using (var cmd = connection.CreateCommand())
							{
								cmd.Transaction = tx;

								var sb = new StringBuilder("INSERT INTO ")
									.Append(commandBuilder.QuoteIdentifier(kvp.Key))
									.Append(" (")
									.Append(commandBuilder.QuoteIdentifier(itemToReplicate.PkName))
									.Append(", ");
								foreach (var column in itemToReplicate.Columns)
								{
									if (column.Key == itemToReplicate.PkName)
										continue;
									sb.Append(commandBuilder.QuoteIdentifier(column.Key)).Append(", ");
								}
								sb.Length = sb.Length - 2;

								var pkParam = cmd.CreateParameter();
								pkParam.ParameterName = GetParameterName(providerFactory, commandBuilder, itemToReplicate.PkName);
								SetParamValue(pkParam, itemToReplicate.Columns[itemToReplicate.PkName]);
								cmd.Parameters.Add(pkParam);

								sb.Append(") \r\nVALUES (")
								  .Append(GetParameterName(providerFactory, commandBuilder, itemToReplicate.PkName))
								  .Append(", ");

								foreach (var column in itemToReplicate.Columns)
								{
									if (column.Key == itemToReplicate.PkName)
										continue;
									var colParam = cmd.CreateParameter();
									colParam.ParameterName = column.Key;
									SetParamValue(colParam, column.Value);
									cmd.Parameters.Add(colParam);
									sb.Append(GetParameterName(providerFactory, commandBuilder, column.Key)).Append(", ");
								}
								sb.Length = sb.Length - 2;
								sb.Append(")");
								cmd.CommandText = sb.ToString();
								cmd.ExecuteNonQuery();
							}
						}
					}
					tx.Commit();
				}
			}
		}

		private static void SetParamValue(DbParameter colParam, RavenJToken val)
		{
			if (val == null)
				colParam.Value = DBNull.Value;
			else
			{
				switch (val.Type)
				{
					case JTokenType.None:
					case JTokenType.Object:
					case JTokenType.Uri:
					case JTokenType.Raw:
					case JTokenType.Array:
						colParam.Value = val.Value<string>();
						return;
					case JTokenType.String:
						var value = val.Value<string>();
						if (value.Length > 0)
						{
							if (char.IsDigit(value[0]))
							{
								DateTime dateTime;
								if (DateTime.TryParseExact(value, Default.OnlyDateTimeFormat, CultureInfo.InvariantCulture,
														   DateTimeStyles.RoundtripKind, out dateTime))
								{
									colParam.Value = dateTime;
									return;
								}
								DateTimeOffset dateTimeOffset;
								if (DateTimeOffset.TryParseExact(value, Default.DateTimeFormatsToRead, CultureInfo.InvariantCulture,
														   DateTimeStyles.RoundtripKind, out dateTimeOffset))
								{
									colParam.Value = dateTimeOffset;
									return;
								}
							}
						}
						colParam.Value = value;
						return;
					case JTokenType.Integer:
					case JTokenType.Date:
					case JTokenType.Bytes:
					case JTokenType.Guid:
					case JTokenType.Boolean:
					case JTokenType.TimeSpan:
					case JTokenType.Float:
						colParam.Value = val.Value<object>();
						return;
					case JTokenType.Null:
					case JTokenType.Undefined:
						colParam.Value = DBNull.Value;
						return;
					default:
						throw new InvalidOperationException("Cannot understand how to save " + val.Type + " for " + colParam.ParameterName);
				}
			}
		}


		private string GetParameterName(DbProviderFactory providerFactory, DbCommandBuilder commandBuilder, string paramName)
		{
			switch (providerFactory.GetType().Name)
			{
				case "SqlClientFactory":
				case "MySqlClientFactory":
					return "@" + paramName;

				case "OracleClientFactory":
				case "NpgsqlFactory":
					return ":" + paramName;

				default:
					// If we don't know, try to get it from the CommandBuilder.
					return getParameterNameFromBuilder(commandBuilder, paramName);
			}
		}

		private static readonly Func<DbCommandBuilder, string, string> getParameterNameFromBuilder =
				(Func<DbCommandBuilder, string, string>)
				Delegate.CreateDelegate(typeof(Func<DbCommandBuilder, string, string>),
										typeof(DbCommandBuilder).GetMethod("GetParameterName",
																		   BindingFlags.Instance | BindingFlags.NonPublic, Type.DefaultBinder,
																		   new[] { typeof(string) }, null));


		public class ItemToReplicate
		{
			private RavenJObject columns;
			public string PkName { get; set; }
			public JsObject Data { get; set; }
			public RavenJObject Columns
			{
				get
				{
					if (columns == null)
						columns = ScriptedJsonPatcher.ToRavenJObject(Data);
					return columns;
				}
			}
		}

		private class SqlReplicationScriptedJsonPatcher : ScriptedJsonPatcher
		{
			private readonly Dictionary<string, List<ItemToReplicate>> dictionary;
			private readonly string docId;

			public SqlReplicationScriptedJsonPatcher(DocumentDatabase database, 
				Dictionary<string, List<ItemToReplicate>> dictionary,
				string docId)
				: base(database)
			{
				this.dictionary = dictionary;
				this.docId = docId;
			}

			protected override void CustomizeEngine(JintEngine jintEngine)
			{
				jintEngine.SetParameter("documentId", docId);
				jintEngine.SetFunction("sqlReplicate", (Action<string, string, JsObject>)((table, pkName, cols) =>
					dictionary.GetOrAdd(table).Add(new ItemToReplicate
					{
						PkName = pkName,
						Data = cols
					})));
			}

			protected override RavenJObject ConvertReturnValue(JsObject jsObject)
			{
				return null;// we don't use / need the return value
			}
		}

		private Guid GetLastEtagFor(SqlReplicationConfig sqlReplicationConfig)
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