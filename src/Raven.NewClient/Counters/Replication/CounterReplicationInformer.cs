using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Connection.Request;
using Raven.NewClient.Client.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.Counters.Replication
{
    public class CounterReplicationInformer
    {
        private readonly static ILog log = LogManager.GetLogger(typeof(CounterReplicationInformer));

        private readonly HttpJsonRequestFactory requestFactory;
        private readonly CounterStore counterStore;
        private readonly CountersConvention countersConvention;
        public const int DefaultIntervalBetweenUpdatesInMinutes = 5;

        private bool currentlyExecuting;
        private bool firstTime;
        private readonly object updateReplicationInformationSyncObj = new object();
        private Task refreshReplicationInformationTask;
        private DateTime lastReplicationUpdate;
        private readonly FailureCounters failureCounters;
        private int currentReadStripingBase;

        public List<CounterReplicationDestination> ReplicationDestinations { get; protected set; }

        private static readonly List<CounterReplicationDestination> Empty = new List<CounterReplicationDestination>();
        private readonly int delayTimeInMiliSec;

        /// <summary>
        /// Gets the replication destinations.
        /// </summary>
        /// <value>The replication destinations.</value>
        public List<CounterReplicationDestination> ReplicationDestinationsAccordingToFailover
        {
            get
            {
                if (CountersConventions.FailoverBehavior == FailoverBehavior.FailImmediately)
                    return Empty;

                return ReplicationDestinations;
            }
        }

        internal CounterReplicationInformer(HttpJsonRequestFactory requestFactory, CounterStore counterStore, CountersConvention countersConvention, int delayTimeInMiliSec = 1000)
        {
            currentReadStripingBase = 0;
            ReplicationDestinations = new List<CounterReplicationDestination>();
            this.requestFactory = requestFactory;
            this.counterStore = counterStore;
            this.countersConvention = countersConvention;
            this.delayTimeInMiliSec = delayTimeInMiliSec;
            failureCounters = new FailureCounters();
            firstTime = true;
            lastReplicationUpdate = SystemTime.UtcNow;
            MaxIntervalBetweenUpdatesInMilliseconds = TimeSpan.FromMinutes(DefaultIntervalBetweenUpdatesInMinutes).TotalMilliseconds;
        }

        internal void OnReplicationUpdate()
        {
            lastReplicationUpdate = SystemTime.UtcNow;
        }

        public async Task<T> ExecuteWithReplicationAsync<T>(string counterStoreUrl, HttpMethod method, Func<string, string, Task<T>> operation, CancellationToken token)
        {
            Debug.Assert(typeof(T).FullName.Contains("Task") == false);

            if (currentlyExecuting && CountersConventions.AllowMultipleAsyncOperations == false)
                throw new InvalidOperationException("Only a single concurrent async request is allowed per async store instance.");

            currentlyExecuting = true;
            try
            {
                var operationCredentials = counterStore.Credentials != null ? 
                    new OperationCredentials(counterStore.Credentials.ApiKey, counterStore.Credentials.Credentials) : 
                    new OperationCredentials(null,CredentialCache.DefaultNetworkCredentials);

                var localReplicationDestinations = ReplicationDestinationsAccordingToFailover; // thread safe copy

                //check for supported flags
                var shouldReadFromAllServers = (CountersConventions.FailoverBehavior & FailoverBehavior.ReadFromAllServers) != 0;
                var shouldFailImmediately = (CountersConventions.FailoverBehavior & FailoverBehavior.FailImmediately) != 0;

                AsyncOperationResult<T> operationResult;

                if (shouldReadFromAllServers && localReplicationDestinations.Count > 0 && !shouldFailImmediately)
                {
                    operationResult = await TryExecuteOperationWithLoadBalancing(counterStoreUrl, operation, token, localReplicationDestinations, operationCredentials).ConfigureAwait(false);
                    if (operationResult.Success)
                        return operationResult.Result;

                    RefreshReplicationInformation(); //force refresh of cluster information -> we failed to do round-robin, maybe some servers are down? 													
                }

                //if we did load balancing and got to this point, this means we failed to connect to the designated server, and then the following
                //logic would server as 'retry' strategy -> failed to do round-robin -> retry nodes one-by-one

                //otherwise we didn't do load balancing; therefore go the usual route -> try to read from primary and if fails -> try to read from secondary

                operationResult = await TryExecuteOperationWithFailover(counterStoreUrl, operation, token, operationCredentials, shouldFailImmediately, localReplicationDestinations).ConfigureAwait(false);

                if (operationResult.Success)
                    return operationResult.Result;

                //maybe it's transient failure? if so, try again
                if (operationResult.Success == false && failureCounters.IsFirstFailure(counterStoreUrl)) 
                {
                    RefreshReplicationInformation();
                    operationResult = await TryExecuteOperationWithFailover(counterStoreUrl, operation, token, operationCredentials, shouldFailImmediately, localReplicationDestinations).ConfigureAwait(false);
                    if (operationResult.Success)
                        return operationResult.Result;
                }

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
                Interlocked.Increment(ref currentReadStripingBase);
                currentlyExecuting = false;
            }
        }

        private async Task<AsyncOperationResult<T>> TryExecuteOperationWithFailover<T>(string counterStoreUrl, Func<string, string, Task<T>> operation, CancellationToken token, OperationCredentials operationCredentials, bool shouldFailImmediately, List<CounterReplicationDestination> localReplicationDestinations)
        {
            var operationResult = await TryExecuteOperationOnPrimaryNode(counterStoreUrl, operation, token, operationCredentials).ConfigureAwait(false);
            if (operationResult.Success)
                return operationResult;

            if (shouldFailImmediately)
                throw new InvalidOperationException(@"Attempted to connect to master and failed. Since there is FailImmediately flag specified in FailoverBehavior, failing the operation.");

            operationResult = await TryExecutingOperationsOnSecondaryNodes(operation, token, localReplicationDestinations, operationCredentials).ConfigureAwait(false);
            if (operationResult.Success)
                return operationResult;

            return new AsyncOperationResult<T>
            {
                Result = default(T),
                Success = false
            };
        }

        private async Task<AsyncOperationResult<T>> TryExecuteOperationWithLoadBalancing<T>(string counterStoreUrl, Func<string, string, Task<T>> operation, CancellationToken token, List<CounterReplicationDestination> localReplicationDestinations, OperationCredentials operationCredentials)
        {
            //essentially do round robin load balancing here
            var replicationIndex = currentReadStripingBase % (localReplicationDestinations.Count + 1);
            AsyncOperationResult<T> operationResult;

            if (ShouldReadFromSecondaryNode(replicationIndex, localReplicationDestinations))
            {
                var storeUrl = localReplicationDestinations[replicationIndex].ServerUrl;
                var storeName = localReplicationDestinations[replicationIndex].CounterStorageName;
                if (ShouldExecuteUsing(storeUrl, storeName, operationCredentials, token))
                {
                    operationResult = await TryExecuteOperationAsync(storeUrl,storeName, operation, true, operationCredentials, token).ConfigureAwait(false);
                    if (operationResult.Success)
                        return operationResult;
                }
            }
            else //read from primary node
            {
                if (ShouldExecuteUsing(counterStoreUrl, counterStore.Name, operationCredentials, token))
                {
                    operationResult = await TryExecuteOperationAsync(counterStoreUrl, counterStore.Name, operation, true, operationCredentials, token).ConfigureAwait(false);
                    if (operationResult.Success)
                        return operationResult;
                }
            }

            return new AsyncOperationResult<T>
            {
                Result = default(T),
                Success = false
            };
        }

        private static bool ShouldReadFromSecondaryNode(int replicationIndex, List<CounterReplicationDestination> localReplicationDestinations)
        {
            // if replicationIndex == destinations count, then we want to use the master
            // if replicationIndex < 0, then we were explicitly instructed to use the master
            return replicationIndex < localReplicationDestinations.Count && replicationIndex > 0;
        }

        public double MaxIntervalBetweenUpdatesInMilliseconds { get; set; }

        public CountersConvention CountersConventions
        {
            get { return countersConvention; }
        }

        private async Task<AsyncOperationResult<T>> TryExecuteOperationAsync<T>(string url, string counterStoreName, Func<string, string, Task<T>> operation, bool avoidThrowing, OperationCredentials credentials, CancellationToken cancellationToken)
        {
            var tryWithPrimaryCredentials = failureCounters.IsFirstFailure(url);
            bool shouldTryAgain = false;

            try
            {
                cancellationToken.ThrowCancellationIfNotDefault(); //canceling the task here potentially will stop the recursion
                var result = await operation(url,counterStoreName).ConfigureAwait(false);
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
            return await TryExecuteOperationAsync(url,counterStoreName, operation, avoidThrowing, credentials, cancellationToken).ConfigureAwait(false);
        }

        private bool ShouldExecuteUsing(string serverUrl, string counterStoreName, OperationCredentials credentials, CancellationToken token)
        {

            var failureCounter = failureCounters.GetHolder(serverUrl);
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
                            var r = await TryExecuteOperationAsync<object>(serverUrl, counterStoreName, async (url, name) =>
                            {
                                var serverCheckUrl = GetServerCheckUrl($"{url}/cs/{name}");
                                var requestParams = new CreateHttpJsonRequestParams(null, serverCheckUrl, HttpMethods.Post, credentials, CountersConventions);
                                using (var request = requestFactory.CreateHttpJsonRequest(requestParams))
                                {
                                    await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                                }
                                return null;

                            }, true, credentials, token).ConfigureAwait(false);
                            if (r.Success)
                            {
                                failureCounters.ResetFailureCount(serverUrl);
                                return;
                            }
                        }
                        catch (ObjectDisposedException)
                        {
                            return; // disposed, nothing to do here
                        }
                        catch (Exception)
                        {
                            // safely ignore and let it try once again
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

        private async Task<AsyncOperationResult<T>> TryExecutingOperationsOnSecondaryNodes<T>(Func<string, string, Task<T>> operation, CancellationToken token, List<CounterReplicationDestination> localReplicationDestinations, OperationCredentials operationCredentials)
        {
            for (var i = 0; i < localReplicationDestinations.Count; i++)
            {
                token.ThrowCancellationIfNotDefault();

                var replicationDestination = localReplicationDestinations[i];
                if (ShouldExecuteUsing(replicationDestination.ServerUrl, replicationDestination.CounterStorageName, operationCredentials, token) == false)
                    continue;

                var hasMoreReplicationDestinations = localReplicationDestinations.Count > i + 1;
                var operationResult = await TryExecuteOperationAsync(replicationDestination.ServerUrl,replicationDestination.CounterStorageName, operation, hasMoreReplicationDestinations, operationCredentials, token).ConfigureAwait(false);

                if (operationResult.Success)
                    return operationResult;

                failureCounters.IncrementFailureCount(replicationDestination.ServerUrl);
                if (!operationResult.WasTimeout && failureCounters.IsFirstFailure(replicationDestination.ServerUrl))
                {
                    operationResult = await TryExecuteOperationAsync(replicationDestination.ServerUrl, replicationDestination.CounterStorageName, operation, hasMoreReplicationDestinations, operationCredentials, token).ConfigureAwait(false);
                    if (operationResult.Success)
                        return operationResult;

                    failureCounters.IncrementFailureCount(replicationDestination.ServerUrl);
                }
            }

            return new AsyncOperationResult<T>
            {
                Result = default(T),
                Success = false
            };
        }

        private async Task<AsyncOperationResult<T>> TryExecuteOperationOnPrimaryNode<T>(string counterStoreUrl, Func<string, string, Task<T>> operation, CancellationToken token, OperationCredentials operationCredentials)
        {
            if (ShouldExecuteUsing(counterStoreUrl, counterStore.Name, operationCredentials, token))
            {
                var operationResult = await TryExecuteOperationAsync(counterStoreUrl, counterStore.Name, operation, true, operationCredentials, token).ConfigureAwait(false);
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

                if (replicationFetchTask.Status != TaskStatus.Faulted)
                    failureCounters.ResetFailureCount(counterStore.Url);

                document = new JsonDocument
                {
                    DataAsJson = RavenJObject.FromObject(replicationFetchTask.Result)
                };
            }
            catch (Exception e)
            {
                log.ErrorException("Could not contact master for fetching replication information. Something is wrong.", e);
                document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
                if (document == null || document.DataAsJson == null)
                    throw;
            }
            ReplicationInformerLocalCache.TrySavingReplicationInformationToLocalCache(serverHash, document);

            UpdateReplicationInformationFromDocument(document);
        }

        public Task UpdateReplicationInformationIfNeededAsync()
        {
            if (CountersConventions.FailoverBehavior == FailoverBehavior.FailImmediately)
                return new CompletedTask();

            var updateInterval = TimeSpan.FromMilliseconds(MaxIntervalBetweenUpdatesInMilliseconds);
            if (lastReplicationUpdate.AddMinutes(updateInterval.TotalMinutes) > SystemTime.UtcNow && firstTime == false)
                return new CompletedTask();

            lock (updateReplicationInformationSyncObj)
            {
                if (!firstTime) //first time the local cache is obviously empty
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
            ReplicationDestinations = document.DataAsJson.Value<RavenJArray>("Destinations")
                                            .Select(x => JsonConvert.DeserializeObject<CounterReplicationDestination>(x.ToString())).ToList();

            foreach (var replicationDestination in ReplicationDestinations)
            {
                FailureCounter value;
                if (failureCounters.FailureCounts.TryGetValue(replicationDestination.ServerUrl, out value))
                    continue;
                failureCounters.FailureCounts[replicationDestination.ServerUrl] = new FailureCounter();
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
