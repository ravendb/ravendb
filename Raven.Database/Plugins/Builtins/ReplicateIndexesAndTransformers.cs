using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Database.Server.Tenancy;
using Raven.Json.Linq;
using Raven.Server;

namespace Raven.Database.Plugins.Builtins
{
	public class ReplicateIndexesAndTransformers : IServerStartupTask
	{
		private RavenDbServer _server;
		private TimeSpan _replicationFrequency;
		private TimeSpan _lastQueriedFrequency;
		private readonly HttpRavenRequestFactory _requestFactory = new HttpRavenRequestFactory();
		private readonly object _lastQuerySendingSyncObject = new object();
		private readonly object _replicationTaskSyncObject = new object();

		private Timer replicationTaskTimer;
		private Timer lastQueriedTaskTimer;

		public void Execute(RavenDbServer ravenServer)
		{
			_server = ravenServer;
			_replicationFrequency = TimeSpan.FromSeconds(_server.Configuration.IndexAndTransformerReplicationLatencyInSec); //by default 10 min
			_lastQueriedFrequency = TimeSpan.FromSeconds(_server.Configuration.TimeToWaitBeforeRunningIdleIndexes.TotalSeconds / 2);

			replicationTaskTimer = _server.SystemDatabase.TimerManager.NewTimer(ReplicateIndexesAndTransformersIfNeeded, TimeSpan.Zero, _replicationFrequency);
			lastQueriedTaskTimer = _server.SystemDatabase.TimerManager.NewTimer(SendLastQueriedIfNeeded, TimeSpan.Zero, _lastQueriedFrequency);
		}

		private void SendLastQueriedIfNeeded(object state)
		{
			//since the latency of these requests is configurable, prevent
			//concurrent execution of this method - in case small execution time is configured
			if (Monitor.TryEnter(_lastQuerySendingSyncObject) == false) //precaution
				return;
			try
			{
				if (_server.Disposed)
				{
					Dispose();
					return;
				}

				var databaseLandLord = _server.Options.DatabaseLandlord;
				var databasesWithReplicationEnabled = FindDatabasesWithReplicationEnabled(databaseLandLord);
				if (databasesWithReplicationEnabled == null) return;

				foreach (var databaseName in databasesWithReplicationEnabled)
				{
					var getDatabaseTask = databaseLandLord.GetDatabaseInternal(databaseName);
					getDatabaseTask.Wait();
					var db = getDatabaseTask.Result;

					if (db == null) //precaution - should never happen
						continue;
					var relevantIndexLastQueries = db.Statistics.Indexes.Where(indexStats => indexStats.IsInvalidIndex == false &&
					                                                                         indexStats.Priority != IndexingPriority.Error &&
					                                                                         indexStats.Priority != IndexingPriority.Disabled &&
					                                                                         indexStats.LastQueryTimestamp.HasValue)						 
						

// ReSharper disable once PossibleInvalidOperationException
						.ToDictionary(indexStats => indexStats.Name, indexStats => indexStats.LastQueryTimestamp.GetValueOrDefault());

					if (relevantIndexLastQueries.Count == 0) continue;

					var destinations = GetReplicationDestinations(db);
					foreach (var destination in destinations.Where(d => d.SkipIndexReplication == false))
						SendLastQueriedToReplicationDestination(destination, relevantIndexLastQueries);
				}
			}
			finally
			{
				Monitor.Exit(_lastQuerySendingSyncObject);
			}
		}

		private void SendLastQueriedToReplicationDestination(ReplicationDestination destination, Dictionary<string, DateTime> relevantIndexLastQueries)
		{
			var connectionOptions = new RavenConnectionStringOptions
			{
				ApiKey = destination.ApiKey,
				Url = destination.Url,
				DefaultDatabase = destination.Database
			};

			var urlTemplate = "{0}/databases/{1}/indexes/last-queried";
			if (!destination.Url.StartsWith("http://", true, CultureInfo.CurrentCulture) &&
				!destination.Url.StartsWith("https://", true, CultureInfo.CurrentCulture))
			{
				urlTemplate = "http://" + urlTemplate;
			}

			var operationUrl = string.Format(urlTemplate, destination.Url, destination.Database);
			var replicationRequest = _requestFactory.Create(operationUrl, "POST", connectionOptions);
			replicationRequest.Write(RavenJObject.FromObject(relevantIndexLastQueries));
			try
			{
				replicationRequest.ExecuteRequest();
			}
			catch
			{
			}
		}

		private void ReplicateIndexesAndTransformersIfNeeded(object state)
		{
			//since the latency of these requests is configurable, prevent
			//concurrent execution of this method
			if (Monitor.TryEnter(_replicationTaskSyncObject) == false)
				return;

			try
			{
				if (_server.Disposed)
				{
					Dispose();
					return;
				}

				var databaseLandLord = _server.Options.DatabaseLandlord;
				var databasesWithReplicationEnabled = FindDatabasesWithReplicationEnabled(databaseLandLord);
				if (databasesWithReplicationEnabled == null) return;

				foreach (var databaseName in databasesWithReplicationEnabled)
				{
					var getDatabaseTask = databaseLandLord.GetDatabaseInternal(databaseName);
					getDatabaseTask.Wait();
					var db = getDatabaseTask.Result;
				
					if(db == null) //precaution - should never happen
						continue;				

					if (db.Indexes.Definitions.Length <= 0 && db.Transformers.Definitions.Length <= 0)
						return;

					var destinations = GetReplicationDestinations(db);
					foreach (var destination in destinations.Where(d => d.SkipIndexReplication == false))
					{
						if (db.Indexes.Definitions.Length > 0)
							ReplicateIndexes(db.Indexes.Definitions, destination);

						if (db.Transformers.Definitions.Length > 0)
							ReplicateTransformers(db.Transformers.Definitions, destination);
					}
				
				}
			}
			finally
			{
				Monitor.Exit(_replicationTaskSyncObject);
			}
		}

		private IEnumerable<string> FindDatabasesWithReplicationEnabled(DatabasesLandlord databaseLandLord)
		{
			var systemDatabase = databaseLandLord.SystemDatabase;
			var databasesWithReplicationEnabled = new HashSet<string>();
			int nextStart = 0;
			var databaseDocuments = systemDatabase.Documents
				.GetDocumentsWithIdStartingWith("Raven/Databases/", null, null, 0, int.MaxValue, systemDatabase.WorkContext.CancellationToken, ref nextStart);

			foreach (var database in databaseDocuments)
			{
				var db = database.JsonDeserialization<DatabaseDocument>();
				var dbName = database.Value<RavenJObject>(Constants.Metadata).Value<string>("@id").Split('/').Last();

				string bundlesList;
				if (db.Settings.TryGetValue(Constants.ActiveBundles, out bundlesList))
				{
					if (bundlesList.IndexOf("Replication", StringComparison.InvariantCultureIgnoreCase) >= 0)
						databasesWithReplicationEnabled.Add(dbName);
				}
			}
			return databasesWithReplicationEnabled;
		}

		private void ReplicateIndexes(IEnumerable<IndexDefinition> definitions, ReplicationDestination destination)
		{
			foreach (var definition in definitions)
				ReplicateIndex(definition.Name, destination, RavenJObject.FromObject(definition), _requestFactory);
		}

		private void ReplicateTransformers(IEnumerable<TransformerDefinition> definitions, ReplicationDestination destination)
		{
			foreach (var definition in definitions)
			{
				var clonedTransformer = definition.Clone();
				clonedTransformer.TransfomerId = 0;
				ReplicateTransformer(definition.Name, destination, RavenJObject.FromObject(clonedTransformer), _requestFactory);
			}
		}

		private void ReplicateIndex(string indexName, ReplicationDestination destination, RavenJObject indexDefinition, HttpRavenRequestFactory httpRavenRequestFactory)
		{
			var connectionOptions = new RavenConnectionStringOptions
			{
				ApiKey = destination.ApiKey,
				Url = destination.Url,
				DefaultDatabase = destination.Database
			};

			if (!String.IsNullOrWhiteSpace(destination.Username) &&
				!String.IsNullOrWhiteSpace(destination.Password))
			{
				connectionOptions.Credentials = new NetworkCredential(destination.Username, destination.Password, destination.Domain ?? string.Empty);
			}

			var urlTemplate = "{0}/databases/{1}/indexes/{2}";
			if (!destination.Url.StartsWith("http://", true, CultureInfo.CurrentCulture) &&
			    !destination.Url.StartsWith("https://", true, CultureInfo.CurrentCulture))
			{
				urlTemplate = "http://" + urlTemplate;
			}

			if (Uri.IsWellFormedUriString(destination.Url, UriKind.RelativeOrAbsolute) == false)
			{
				return;
			}

			var operationUrl = string.Format(urlTemplate, destination.Url, destination.Database, Uri.EscapeUriString(indexName));
			var replicationRequest = httpRavenRequestFactory.Create(operationUrl, "PUT", connectionOptions);
			replicationRequest.Write(indexDefinition);

			try
			{
				replicationRequest.ExecuteRequest();
			}
			catch
			{
			}
		}

		private void ReplicateTransformer(string transformerName, ReplicationDestination destination, RavenJObject transformerDefinition, HttpRavenRequestFactory httpRavenRequestFactory)
		{
			var connectionOptions = new RavenConnectionStringOptions
			{
				ApiKey = destination.ApiKey,
				Url = destination.Url,
				DefaultDatabase = destination.Database
			};

			if (!String.IsNullOrWhiteSpace(destination.Username) &&
				!String.IsNullOrWhiteSpace(destination.Password))
			{
				connectionOptions.Credentials = new NetworkCredential(destination.Username, destination.Password, destination.Domain ?? string.Empty);
			}

			var urlTemplate = "{0}/databases/{1}/transformers/{2}";
			if (!destination.Url.StartsWith("http://", true, CultureInfo.CurrentCulture) &&
				!destination.Url.StartsWith("https://", true, CultureInfo.CurrentCulture))
			{
				urlTemplate = "http://" + urlTemplate;
			}

			if (Uri.IsWellFormedUriString(destination.Url, UriKind.RelativeOrAbsolute) == false)
			{
				return;
			}

			var operationUrl = string.Format(urlTemplate, destination.Url, destination.Database, Uri.EscapeUriString(transformerName));
			var replicationRequest = httpRavenRequestFactory.Create(operationUrl, "PUT", connectionOptions);
			replicationRequest.Write(transformerDefinition);

			try
			{
				replicationRequest.ExecuteRequest();
			}
			catch 
			{
			}
		}

		private IEnumerable<ReplicationDestination> GetReplicationDestinations(DocumentDatabase database)
		{
			var document = database.Documents.Get(Constants.RavenReplicationDestinations, null);
			if (document == null)
			{
				return new ReplicationDestination[0];
			}
			ReplicationDocument deserializedReplicationDocument;
			try
			{
				deserializedReplicationDocument = document.DataAsJson.JsonDeserialization<ReplicationDocument>();
			}
			catch (Exception)
			{
				return new ReplicationDestination[0];
			}

			if (string.IsNullOrWhiteSpace(deserializedReplicationDocument.Source))
			{
				deserializedReplicationDocument.Source = database.TransactionalStorage.Id.ToString();
				try
				{
					var ravenJObject = RavenJObject.FromObject(deserializedReplicationDocument);
					ravenJObject.Remove("Id");
					database.Documents.Put(Constants.RavenReplicationDestinations, document.Etag, ravenJObject, document.Metadata, null);
				}
				catch (ConcurrencyException)
				{
					// we will get it next time
				}
			}

			return deserializedReplicationDocument.Destinations;
		}

		public void Dispose()
		{
			replicationTaskTimer.Dispose();
			lastQueriedTaskTimer.Dispose();
		}
	}
}
