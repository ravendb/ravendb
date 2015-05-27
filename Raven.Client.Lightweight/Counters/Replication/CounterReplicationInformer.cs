using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Connection;
using Raven.Client.Connection.Request;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Counters.Replication
{
	public class CounterReplicationInformer : ReplicationInformerBase<CountersClient>,ICountersReplicationInformer
	{
		private bool currentlyExecuting;
		private int requestCount;
		private bool firstTime;
		private readonly object updateReplicationInformationSyncObj = new object();
		private Task refreshReplicationInformationTask;
		private DateTime lastReplicationUpdate;

		public CounterReplicationInformer(Client.Convention conventions, HttpJsonRequestFactory requestFactory, int delayTime = 1000) : base(conventions, requestFactory, delayTime)
		{
			firstTime = true;
			lastReplicationUpdate = SystemTime.UtcNow;
			MaxIntervalBetweenUpdatesInMillisec = TimeSpan.FromMinutes(5).TotalMilliseconds;
		}

		internal void OnReplicationUpdate()
		{
			lastReplicationUpdate = SystemTime.UtcNow;
		}

		public async Task<T> ExecuteWithReplicationAsyncWithReturnValue<T>(HttpMethod method, CountersClient client, Func<OperationMetadata, Task<T>> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			if (currentlyExecuting && Conventions.AllowMultipuleAsyncOperations == false)
				throw new InvalidOperationException("Only a single concurrent async request is allowed per async client instance.");

			currentlyExecuting = true;
			try
			{
				var operationCredentials = new OperationCredentials(client.ApiKey, client.Credentials);
				return await ExecuteWithReplicationAsync(method, client.ServerUrl, operationCredentials, null, currentRequest, 0, operation)
										.ConfigureAwait(false);
			}
			catch (AggregateException e)
			{
				var singleException = e.ExtractSingleInnerException();
				if (singleException != null)
					throw singleException;

				throw;
			}
			finally
			{
				currentlyExecuting = false;
			}
		}
		
		public double MaxIntervalBetweenUpdatesInMillisec { get; set; }

		public async Task ExecuteWithReplicationAsync<T>(HttpMethod method,CountersClient client, Func<OperationMetadata, Task<T>> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			if (currentlyExecuting && Conventions.AllowMultipuleAsyncOperations == false)
				throw new InvalidOperationException("Only a single concurrent async request is allowed per async client instance.");
			
			currentlyExecuting = true;
			try
			{
				var operationCredentials = new OperationCredentials(client.ApiKey, client.Credentials);
				await ExecuteWithReplicationAsync(method, client.ServerUrl, operationCredentials, null, currentRequest, 0, operation)
										.ConfigureAwait(false);
			}
			catch (AggregateException e)
			{
				var singleException = e.ExtractSingleInnerException();
				if (singleException != null)
					throw singleException;

				throw;
			}
			finally
			{
				currentlyExecuting = false;
			}
		}


		public override void RefreshReplicationInformation(CountersClient client)
		{
			JsonDocument document;
			var serverHash = ServerHash.GetServerHash(client.ServerUrl);
			try
			{
				var replicationFetchTask = client.Replication.GetReplicationsAsync();
				replicationFetchTask.Wait();

				if(replicationFetchTask.Status != TaskStatus.Faulted)
					FailureCounters.ResetFailureCount(client.ServerUrl);

				document = new JsonDocument
				{
					DataAsJson = RavenJObject.FromObject(replicationFetchTask.Result)
				};
			}
			catch (Exception e)
			{
				Log.ErrorException("Could not contact master for fetching replication information. Something is wrong.",e);
				document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
			}

			if (document == null || document.DataAsJson == null)
				return;

			ReplicationInformerLocalCache.TrySavingReplicationInformationToLocalCache(serverHash, document);

			UpdateReplicationInformationFromDocument(document);
		}

		public Task UpdateReplicationInformationIfNeededAsync(CountersClient client)
		{
			if (Conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
				return new CompletedTask();

			var updateInterval = TimeSpan.FromMilliseconds(MaxIntervalBetweenUpdatesInMillisec);
			if (lastReplicationUpdate.AddMinutes(updateInterval.TotalMinutes) > SystemTime.UtcNow && firstTime == false)
				return new CompletedTask();

			lock (updateReplicationInformationSyncObj)
			{
				if (firstTime)
				{
					var serverHash = ServerHash.GetServerHash(client.ServerUrl);

					var document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
					if (IsInvalidDestinationsDocument(document) == false)
						UpdateReplicationInformationFromDocument(document);
				}

				firstTime = false;

				var taskCopy = refreshReplicationInformationTask;
				if (taskCopy != null)
					return taskCopy;

				return refreshReplicationInformationTask = 
					Task.Factory.StartNew(() => RefreshReplicationInformation(client))
					.ContinueWith(task =>
					{
						if (task.Exception != null)
						{
							Log.ErrorException("Failed to refresh replication information", task.Exception);
						}
						lastReplicationUpdate = SystemTime.UtcNow;
						refreshReplicationInformationTask = null;
					});
			}			
		}

		public override void ClearReplicationInformationLocalCache(CountersClient client)
		{
			var serverHash = ServerHash.GetServerHash(client.ServerUrl);
			ReplicationInformerLocalCache.ClearReplicationInformationFromLocalCache(serverHash);
		}

		protected override void UpdateReplicationInformationFromDocument(JsonDocument document)
		{
			var destinations = document.DataAsJson.Value<RavenJArray>("Destinations").Select(x => JsonConvert.DeserializeObject<CounterReplicationDestination>(x.ToString()));

			ReplicationDestinations = destinations.Select(x =>
			{
				ICredentials credentials = null;
				if (string.IsNullOrEmpty(x.Username) == false)
				{
					credentials = string.IsNullOrEmpty(x.Domain)
									  ? new NetworkCredential(x.Username, x.Password)
									  : new NetworkCredential(x.Username, x.Password, x.Domain);
				}

				return new OperationMetadata(x.ServerUrl, new OperationCredentials(x.ApiKey, credentials), null);
			})
				// filter out replication destination that don't have the url setup, we don't know how to reach them
				// so we might as well ignore them. Probably private replication destination (using connection string names only)
			.Where(x => x != null)
			.ToList();

			foreach (var replicationDestination in ReplicationDestinations)
			{
				FailureCounter value;
				if (FailureCounters.FailureCounts.TryGetValue(replicationDestination.Url, out value))
					continue;
				FailureCounters.FailureCounts[replicationDestination.Url] = new FailureCounter();
			}
		}

		//cs/{counterStorageName}/replication/heartbeat
		protected override string GetServerCheckUrl(string baseUrl)
		{
			return baseUrl + "/replication/heartbeat";
		}
	}
}
