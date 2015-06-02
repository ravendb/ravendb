using System;
using System.Collections.Generic;
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
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;

namespace Raven.Client.Counters.Replication
{
	public class CounterReplicationInformer
	{
		private readonly HttpJsonRequestFactory requestFactory;
		private readonly CounterStore counterStore;
		private readonly Convention convention;
		private readonly CounterStore parent;
		public const int DefaultIntervalBetweenUpdatesInMinutes = 5;

		private bool currentlyExecuting;
		private int requestCount;
		private bool firstTime;
		private readonly object updateReplicationInformationSyncObj = new object();
		private Task refreshReplicationInformationTask;
		private DateTime lastReplicationUpdate;
		private readonly ILog log = LogManager.GetCurrentClassLogger();
		private readonly FailureCounters failureCounters;
		private int currentReadStripingBase;

		public List<OperationMetadata> ReplicationDestinations { get; protected set; }

		private static readonly List<OperationMetadata> Empty = new List<OperationMetadata>();
		private readonly int delayTimeInMiliSec;

		/// <summary>
		/// Gets the replication destinations.
		/// </summary>
		/// <value>The replication destinations.</value>
		public List<OperationMetadata> ReplicationDestinationsUrls
		{
			get
			{
				if (Conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
					return Empty;

				return ReplicationDestinations
					.Select(operationMetadata => new OperationMetadata(operationMetadata))
					.ToList();
			}
		}

		internal CounterReplicationInformer(HttpJsonRequestFactory requestFactory, CounterStore counterStore,Convention convention, int delayTimeInMiliSec = 1000)
		{
			currentReadStripingBase = 0;
			ReplicationDestinations = new List<OperationMetadata>();
			this.requestFactory = requestFactory;
			this.counterStore = counterStore;
			this.convention = convention;
			this.delayTimeInMiliSec = delayTimeInMiliSec;
			failureCounters = new FailureCounters();
			firstTime = true;
			lastReplicationUpdate = SystemTime.UtcNow;
			MaxIntervalBetweenUpdatesInMillisec = TimeSpan.FromMinutes(DefaultIntervalBetweenUpdatesInMinutes).TotalMilliseconds;
		}

		internal void OnReplicationUpdate()
		{
			lastReplicationUpdate = SystemTime.UtcNow;
		}

		public async Task<T> ExecuteWithReplicationAsync<T>(string counterStoreUrl, HttpMethod method, Func<string,Task<T>> operation, CancellationToken token)
		{
			if (currentlyExecuting && Conventions.AllowMultipuleAsyncOperations == false)
				throw new InvalidOperationException("Only a single concurrent async request is allowed per async store instance.");

			currentlyExecuting = true;
			try
			{
				var operationCredentials = new OperationCredentials(counterStore.Credentials.ApiKey, counterStore.Credentials.Credentials);
				var localReplicationDestinations = ReplicationDestinationsUrls; // thread safe copy

				var shouldReadFromAllServers = Conventions.FailoverBehavior.HasFlag(FailoverBehavior.ReadFromAllServers);

				AsyncOperationResult<T> operationResult;
				if (shouldReadFromAllServers)
				{
					var replicationIndex = currentReadStripingBase % (localReplicationDestinations.Count + 1);
					Interlocked.Increment(ref currentReadStripingBase);

					if (ShouldReadFromSecondaryNode(replicationIndex, localReplicationDestinations))
					{
						var storeUrl = localReplicationDestinations[replicationIndex].Url;
						if (ShouldExecuteUsing(storeUrl, operationCredentials, token))
						{
							operationResult = await TryExecuteOperationAsync(storeUrl, operation, true, operationCredentials, token).ConfigureAwait(false);
							if (operationResult.Success)
								return operationResult.Result;
						}
					}
				}

				operationResult = await TryExecuteOperationOnPrimaryNode(counterStoreUrl, operation, token, operationCredentials);
				if (operationResult.Success)
					return operationResult.Result;

				operationResult = await TryExecutingOperationsOnFailoverNodes(operation, token, localReplicationDestinations, operationCredentials);
				if (operationResult.Success)
					return operationResult.Result;

				// this should not be thrown, but sometimes, things go _really_ wrong...
				throw new InvalidOperationException(@"Attempted to connect to master and all replicas have failed, giving up.
						There is a high probability of a network problem preventing access to all the replicas.
						Failed to get in touch with any of the " + (1 + localReplicationDestinations.Count) + " Counter instances.");

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

		private static bool ShouldReadFromSecondaryNode(int replicationIndex, List<OperationMetadata> localReplicationDestinations)
		{
			// if replicationIndex == destinations count, then we want to use the master
			// if replicationIndex < 0, then we were explicitly instructed to use the master
			return replicationIndex < localReplicationDestinations.Count && replicationIndex >= 0;
		}

		public double MaxIntervalBetweenUpdatesInMillisec { get; set; }
		public Convention Conventions
		{
			get { return convention; }
		}

		private async Task<AsyncOperationResult<T>> TryExecuteOperationAsync<T>(string url, Func<string,Task<T>> operation, bool avoidThrowing, OperationCredentials credentials, CancellationToken cancellationToken)
		{
			var tryWithPrimaryCredentials = failureCounters.IsFirstFailure(url);
			bool shouldTryAgain = false;

			try
			{
				cancellationToken.ThrowCancellationIfNotDefault(); //canceling the task here potentially will stop the recursion
				var result = await operation(url).ConfigureAwait(false);
				failureCounters.ResetFailureCount(url);
				return new AsyncOperationResult<T>
				{
					Result = result,
					Success = true
				};
			}
			catch (Exception e)
			{
				var ae = e as AggregateException;
				ErrorResponseException errorResponseException;
				if (ae != null)
				{
					errorResponseException = ae.ExtractSingleInnerException() as ErrorResponseException;
				}
				else
				{
					errorResponseException = e as ErrorResponseException;
				}
				if (tryWithPrimaryCredentials && credentials.HasCredentials() && errorResponseException != null)
				{
					failureCounters.IncrementFailureCount(url);

					if (errorResponseException.StatusCode == HttpStatusCode.Unauthorized)
					{
						shouldTryAgain = true;
					}
				}

				if (shouldTryAgain == false)
				{
					if (avoidThrowing == false)
						throw;

					bool wasTimeout;
					var isServerDown = HttpConnectionHelper.IsServerDown(e, out wasTimeout);

					if (avoidThrowing == false)
						throw;

					if (e.Data.Contains(Constants.RequestFailedExceptionMarker) && isServerDown)
					{
						return new AsyncOperationResult<T>
						{
							Success = false,
							WasTimeout = wasTimeout,
							Error = e
						};
					}

					if (isServerDown)
					{
						return new AsyncOperationResult<T>
						{
							Success = false,
							WasTimeout = wasTimeout,
							Error = e
						};
					}
					throw;
				}
			}
			return await TryExecuteOperationAsync(url,operation,avoidThrowing,credentials,cancellationToken);
		}

		private bool ShouldExecuteUsing(string counterStoreUrl, OperationCredentials credentials, CancellationToken token)
		{

			var failureCounter = failureCounters.GetHolder(counterStoreUrl);
			if (failureCounter.Value == 0)
				return true;

			if (failureCounter.ForceCheck)
				return true;

			var currentTask = failureCounter.CheckDestination;
			if (currentTask.Status != TaskStatus.Running && delayTimeInMiliSec > 0)
			{
				var checkDestination = new Task(async delegate
				{
					for (int i = 0; i < 3; i++)
					{
						token.ThrowCancellationIfNotDefault();
						try
						{
							var r = await TryExecuteOperationAsync<object>(counterStoreUrl, async url =>
							{
								var requestParams = new CreateHttpJsonRequestParams(null, GetServerCheckUrl(url), HttpMethods.Get, credentials, Conventions.ShouldCacheRequest);
								using (var request = requestFactory.CreateHttpJsonRequest(requestParams))
								{
									await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
								}
								return null;

							},false,credentials,token).ConfigureAwait(false);
							if (r.Success)
							{
								failureCounters.ResetFailureCount(counterStoreUrl);
								return;
							}
						}
						catch (ObjectDisposedException)
						{
							return; // disposed, nothing to do here
						}
						await Task.Delay(delayTimeInMiliSec, token).ConfigureAwait(false);
					}
				});

				var old = Interlocked.CompareExchange(ref failureCounter.CheckDestination, checkDestination, currentTask);
				if (old == currentTask)
				{
					checkDestination.Start(TaskScheduler.Default);
				}
			}

			return false;
		}

		private async Task<AsyncOperationResult<T>> TryExecutingOperationsOnFailoverNodes<T>(Func<string, Task<T>> operation, CancellationToken token, List<OperationMetadata> localReplicationDestinations, OperationCredentials operationCredentials)
		{
			for (var i = 0; i < localReplicationDestinations.Count; i++)
			{
				token.ThrowCancellationIfNotDefault();

				var replicationDestination = localReplicationDestinations[i];
				if (ShouldExecuteUsing(replicationDestination.Url, operationCredentials, token) == false)
					continue;

				var hasMoreReplicationDestinations = localReplicationDestinations.Count > i + 1;
				var operationResult = await TryExecuteOperationAsync(replicationDestination.Url, operation, hasMoreReplicationDestinations, operationCredentials, token).ConfigureAwait(false);

				if (operationResult.Success)
					return operationResult;

				failureCounters.IncrementFailureCount(replicationDestination.Url);
				if (!operationResult.WasTimeout && failureCounters.IsFirstFailure(replicationDestination.Url))
				{
					operationResult = await TryExecuteOperationAsync(replicationDestination.Url, operation, hasMoreReplicationDestinations, operationCredentials, token).ConfigureAwait(false);
					if (operationResult.Success)
						return operationResult;

					failureCounters.IncrementFailureCount(replicationDestination.Url);
				}
			}

			return new AsyncOperationResult<T>
			{
				Result = default(T),
				Success = false
			};
		}

		private async Task<AsyncOperationResult<T>> TryExecuteOperationOnPrimaryNode<T>(string counterStoreUrl, Func<string,Task<T>> operation, CancellationToken token, OperationCredentials operationCredentials)
		{
			if (ShouldExecuteUsing(counterStoreUrl, operationCredentials, token))
			{
				var operationResult = await TryExecuteOperationAsync(counterStoreUrl, operation, true, operationCredentials, token).ConfigureAwait(false);
				if (operationResult.Success)
					return operationResult;

				failureCounters.IncrementFailureCount(counterStoreUrl);
			}

			return new AsyncOperationResult<T>
			{
				Result = default(T),
				Success = false
			};
		}


		//TODO: When counter replication will be refactored (simplified) -> the parameter should be removed; now its a constraint of the interface
		public void RefreshReplicationInformation()
		{
			JsonDocument document;
			var serverHash = ServerHash.GetServerHash(counterStore.Url);
			try
			{
				var replicationFetchTask = counterStore.GetReplicationsAsync();
				replicationFetchTask.Wait();

				if(replicationFetchTask.Status != TaskStatus.Faulted)
					failureCounters.ResetFailureCount(counterStore.Url);

				document = new JsonDocument
				{
					DataAsJson = RavenJObject.FromObject(replicationFetchTask.Result)
				};
			}
			catch (Exception e)
			{
				log.ErrorException("Could not contact master for fetching replication information. Something is wrong.",e);
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
					Task.Factory.StartNew(RefreshReplicationInformation)
					.ContinueWith(task =>
					{
						if (task.Exception != null)
						{
							log.ErrorException("Failed to refresh replication information", task.Exception);
						}
						lastReplicationUpdate = SystemTime.UtcNow;
						refreshReplicationInformationTask = null;
					});
			}			
		}

		protected void UpdateReplicationInformationFromDocument(JsonDocument document)
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
				if (failureCounters.FailureCounts.TryGetValue(replicationDestination.Url, out value))
					continue;
				failureCounters.FailureCounts[replicationDestination.Url] = new FailureCounter();
			}
		}

		protected static bool IsInvalidDestinationsDocument(JsonDocument document)
		{
			return document == null ||
				   document.DataAsJson.ContainsKey("Destinations") == false ||
				   document.DataAsJson["Destinations"] == null ||
				   document.DataAsJson["Destinations"].Type == JTokenType.Null;
		}

		//cs/{counterStorageName}/replication/heartbeat
		protected string GetServerCheckUrl(string baseUrl)
		{
			return baseUrl + "/replication/heartbeat";
		}
	}
}
