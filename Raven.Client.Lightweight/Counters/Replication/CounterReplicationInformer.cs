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
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Client.Counters.Replication
{
	public class CounterReplicationInformer : ReplicationInformerBase<CounterStore>
	{
		private readonly CounterStore counterStore;
		public const int DefaultIntervalBetweenUpdatesInMinutes = 5;

		private bool currentlyExecuting;
		private int requestCount;
		private bool firstTime;
		private readonly object updateReplicationInformationSyncObj = new object();
		private Task refreshReplicationInformationTask;
		private DateTime lastReplicationUpdate;

		public CounterReplicationInformer(HttpJsonRequestFactory requestFactory, CounterStore counterStore,int delayTime = 1000) :
			base(new DocumentConvention(), requestFactory, delayTime) //TODO : this will be fixed when replication informer will be rewritten not to depend on ReplicationInformerBase
		{
			this.counterStore = counterStore;
			firstTime = true;
			lastReplicationUpdate = SystemTime.UtcNow;
			MaxIntervalBetweenUpdatesInMillisec = TimeSpan.FromMinutes(DefaultIntervalBetweenUpdatesInMinutes).TotalMilliseconds;
		}

		internal void OnReplicationUpdate()
		{
			lastReplicationUpdate = SystemTime.UtcNow;
		}	

		public async Task<T> ExecuteWithReplicationAsyncWithReturnValue<T>(HttpMethod method, Func<OperationMetadata, Task<T>> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			if (currentlyExecuting && Conventions.AllowMultipuleAsyncOperations == false)
				throw new InvalidOperationException("Only a single concurrent async request is allowed per async store instance.");

			currentlyExecuting = true;
			try
			{
				var operationCredentials = new OperationCredentials(counterStore.Credentials.ApiKey, counterStore.Credentials.Credentials);
				return await ExecuteWithReplicationAsync(method, counterStore.Url, operationCredentials, null, currentRequest, 0, operation)
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

		public async Task<T> ExecuteWithReplicationAsyncWithReturnValue<T>(HttpMethod method, CounterStore store, Func<OperationMetadata, Task<T>> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			if (currentlyExecuting && Conventions.AllowMultipuleAsyncOperations == false)
				throw new InvalidOperationException("Only a single concurrent async request is allowed per async store instance.");

			currentlyExecuting = true;
			try
			{
				var operationCredentials = new OperationCredentials(store.Credentials.ApiKey, store.Credentials.Credentials);
				return await ExecuteWithReplicationAsync(method, store.Url, operationCredentials, null, currentRequest, 0, operation)
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

		public async Task ExecuteWithReplicationAsync<T>(HttpMethod method, Func<OperationMetadata, Task<T>> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			if (currentlyExecuting && Conventions.AllowMultipuleAsyncOperations == false)
				throw new InvalidOperationException("Only a single concurrent async request is allowed per async store instance.");

			currentlyExecuting = true;
			try
			{
				var operationCredentials = new OperationCredentials(counterStore.Credentials.ApiKey, counterStore.Credentials.Credentials);
				await ExecuteWithReplicationAsync(method, counterStore.Url, operationCredentials, null, currentRequest, 0, operation)
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

		public async Task ExecuteWithReplicationAsync<T>(HttpMethod method,CounterStore store, Func<OperationMetadata, Task<T>> operation)
		{
			var currentRequest = Interlocked.Increment(ref requestCount);
			if (currentlyExecuting && Conventions.AllowMultipuleAsyncOperations == false)
				throw new InvalidOperationException("Only a single concurrent async request is allowed per async store instance.");
			
			currentlyExecuting = true;
			try
			{
				var operationCredentials = new OperationCredentials(store.Credentials.ApiKey, store.Credentials.Credentials);
				await ExecuteWithReplicationAsync(method, store.Url, operationCredentials, null, currentRequest, 0, operation)
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

		
		//TODO: When counter replication will be refactored (simplified) -> the parameter should be removed; now its a constraint of the interface
		public override void RefreshReplicationInformation(CounterStore store)
		{
			JsonDocument document;
			var serverHash = ServerHash.GetServerHash(counterStore.Url);
			try
			{
				var replicationFetchTask = store.GetReplicationsAsync();
				replicationFetchTask.Wait();

				if(replicationFetchTask.Status != TaskStatus.Faulted)
					FailureCounters.ResetFailureCount(counterStore.Url);

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

		public Task UpdateReplicationInformationIfNeededAsync()
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
					var serverHash = ServerHash.GetServerHash(counterStore.Url);

					var document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
					if (IsInvalidDestinationsDocument(document) == false)
						UpdateReplicationInformationFromDocument(document);
				}

				firstTime = false;

				var taskCopy = refreshReplicationInformationTask;
				if (taskCopy != null)
					return taskCopy;

				return refreshReplicationInformationTask = 
					Task.Factory.StartNew(() => RefreshReplicationInformation(null))
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

		public override void ClearReplicationInformationLocalCache(CounterStore store)
		{
			var serverHash = ServerHash.GetServerHash(store.Url);
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
