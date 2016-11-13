//-----------------------------------------------------------------------
// <copyright file="ReplicationInformer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Net.Http;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.NewClient.Client.Connection.Request;
using Raven.NewClient.Client.Metrics;
using Raven.Imports.Newtonsoft.Json.Linq;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.NewClient.Client.Data;

namespace Raven.NewClient.Client.Connection
{
    /// <summary>
    /// Replication and failover management on the client side
    /// </summary>
    public abstract class ReplicationInformerBase<TClient> : IReplicationInformerBase<TClient>
    {
        protected readonly ILog Log = LogManager.GetLogger(typeof(ReplicationInformerBase<TClient>));

        protected readonly QueryConvention Conventions;

        private readonly HttpJsonRequestFactory requestFactory;

        private static readonly List<OperationMetadata> Empty = new List<OperationMetadata>();

        private static int readStripingBase;

        /// <summary>
        /// Notify when the failover status changed
        /// </summary>
        public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged
        {
            add
            {
                FailureCounters.FailoverStatusChanged += value;
            }
            remove
            {
                FailureCounters.FailoverStatusChanged -= value;
            }
        }

        public int DelayTimeInMiliSec { get;  set; }

        public List<OperationMetadata> ReplicationDestinations { get; protected set; }

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

        protected ReplicationInformerBase(QueryConvention conventions, HttpJsonRequestFactory requestFactory, int delayTime = 1000)
        {
            Conventions = conventions;
            this.requestFactory = requestFactory;
            ReplicationDestinations = new List<OperationMetadata>();
            DelayTimeInMiliSec = delayTime;
            FailureCounters = new FailureCounters();
        }

        public FailureCounters FailureCounters { get; private set; }

        /// <summary>
        /// Refreshes the replication information.
        /// Expert use only.
        /// </summary>
        public abstract void RefreshReplicationInformation(TClient client);

        public abstract void ClearReplicationInformationLocalCache(TClient client);

        protected abstract void UpdateReplicationInformationFromDocument(JsonDocument document);

        /// <summary>
        /// Should execute the operation using the specified operation URL
        /// </summary>
        private bool ShouldExecuteUsing(OperationMetadata operationMetadata, OperationMetadata primaryOperation, HttpMethod method, bool primary, Exception error, CancellationToken token)
        {
            if (primary == false)
                AssertValidOperation(method, error);

            var failureCounter = FailureCounters.GetHolder(operationMetadata.Url);
            if (failureCounter.Value == 0)
                return true;

            if (failureCounter.ForceCheck)
                return true;

            var currentTask = failureCounter.CheckDestination;
            if (currentTask.Status != TaskStatus.Running && DelayTimeInMiliSec > 0)
            {
                var checkDestination = new Task(async delegate
                {
                    for (int i = 0; i < 3; i++)
                        {
                token.ThrowCancellationIfNotDefault();
                try
                {
                    var r = await TryOperationAsync<object>(async metadata =>
                    {
                                var requestParams = new CreateHttpJsonRequestParams(null, GetServerCheckUrl(metadata.Url), HttpMethods.Get, metadata.Credentials, Conventions);
                        using (var request = requestFactory.CreateHttpJsonRequest(requestParams))
                        {
                            await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                        }
                        return null;
                    }, operationMetadata, primaryOperation, true, token).ConfigureAwait(false);
                    if (r.Success)
                    {
                                FailureCounters.ResetFailureCount(operationMetadata.Url);
                        return;
                    }
                }
                catch (ObjectDisposedException)
                {
                            return; // disposed, nothing to do here
                }
                await Task.Delay(DelayTimeInMiliSec, token).ConfigureAwait(false);
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

        protected abstract string GetServerCheckUrl(string baseUrl);

        protected void AssertValidOperation(HttpMethod method, Exception e)
        {
            switch (Conventions.FailoverBehaviorWithoutFlags)
            {
                case FailoverBehavior.AllowReadsFromSecondaries:
                case FailoverBehavior.AllowReadFromSecondariesWhenRequestTimeThresholdIsSurpassed:
                    if (method == HttpMethods.Get || method == HttpMethod.Head)
                        return;
                    break;
                case FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries:
                    return;
                case FailoverBehavior.FailImmediately:
                    var allowReadFromAllServers = Conventions.FailoverBehavior.HasFlag(FailoverBehavior.ReadFromAllServers);
                    if (allowReadFromAllServers && (method == HttpMethods.Get || method == HttpMethod.Head))
                        return;
                    break;
            }
            throw new InvalidOperationException("Could not replicate " + method +
                                                " operation to secondary node, failover behavior is: " +
                                                Conventions.FailoverBehavior, e);
        }

        protected static bool IsInvalidDestinationsDocument(JsonDocument document)
        {
            return document == null ||
                   document.DataAsJson.ContainsKey("Destinations") == false ||
                   document.DataAsJson["Destinations"] == null ||
                   document.DataAsJson["Destinations"].Type == JTokenType.Null;
        }

        public virtual int GetReadStripingBase(bool increment)
        {
            return increment ? Interlocked.Increment(ref readStripingBase) : readStripingBase;
        }

        public async Task<T> ExecuteWithReplicationAsync<T>(HttpMethod method,
            string primaryUrl,
            OperationCredentials primaryCredentials,
            RequestTimeMetric primaryRequestTimeMetric,
            int currentRequest,
            int currentReadStripingBase,
            Func<OperationMetadata, Task<T>> operation,
            CancellationToken token = default (CancellationToken))
        {
            Debug.Assert(typeof(T).FullName.Contains("Task") == false);

            var localReplicationDestinations = ReplicationDestinationsUrls; // thread safe copy
            var primaryOperation = new OperationMetadata(primaryUrl, primaryCredentials, null);

            var operationResult = new AsyncOperationResult<T>();
            var shouldReadFromAllServers = Conventions.FailoverBehavior.HasFlag(FailoverBehavior.ReadFromAllServers);

            var allowReadFromSecondariesWhenRequestTimeThresholdIsPassed = Conventions.FailoverBehavior.HasFlag(FailoverBehavior.AllowReadFromSecondariesWhenRequestTimeThresholdIsSurpassed);

            if (method == HttpMethods.Get && (shouldReadFromAllServers || allowReadFromSecondariesWhenRequestTimeThresholdIsPassed))
            {
                var replicationIndex = -1;
                if (allowReadFromSecondariesWhenRequestTimeThresholdIsPassed && primaryRequestTimeMetric != null && primaryRequestTimeMetric.RateSurpassed(Conventions))
                    replicationIndex = currentReadStripingBase % (localReplicationDestinations.Count);
                else if (shouldReadFromAllServers)
                    replicationIndex = currentReadStripingBase % (localReplicationDestinations.Count + 1);

                // if replicationIndex == destinations count, then we want to use the master
                // if replicationIndex < 0, then we were explicitly instructed to use the master
                if (replicationIndex < localReplicationDestinations.Count && replicationIndex >= 0)
                {
                    // if it is failing, ignore that, and move to the master or any of the replicas
                    if (ShouldExecuteUsing(localReplicationDestinations[replicationIndex], primaryOperation, method, false, null, token))
                    {
                        operationResult = await TryOperationAsync(operation, localReplicationDestinations[replicationIndex], primaryOperation, true, token).ConfigureAwait(false);
                        if (operationResult.Success)
                            return operationResult.Result;
                    }
                }
            }

            if (ShouldExecuteUsing(primaryOperation,primaryOperation, method, true, null,token))
            {
                operationResult = await TryOperationAsync(operation, primaryOperation, null, !operationResult.WasTimeout && localReplicationDestinations.Count > 0, token)
                    .ConfigureAwait(false);

                if (operationResult.Success)
                    return operationResult.Result;

                FailureCounters.IncrementFailureCount(primaryOperation.Url);
                if (!operationResult.WasTimeout && FailureCounters.IsFirstFailure(primaryOperation.Url))
                {
                    operationResult = await TryOperationAsync(operation, primaryOperation, null, localReplicationDestinations.Count > 0, token).ConfigureAwait(false);

                    if (operationResult.Success)
                        return operationResult.Result;
                    FailureCounters.IncrementFailureCount(primaryOperation.Url);
                }
            }

            for (var i = 0; i < localReplicationDestinations.Count; i++)
            {
                token.ThrowCancellationIfNotDefault();

                var replicationDestination = localReplicationDestinations[i];
                if (ShouldExecuteUsing(replicationDestination, primaryOperation, method, false, operationResult.Error,token) == false)
                    continue;

                var hasMoreReplicationDestinations = localReplicationDestinations.Count > i + 1;
                operationResult = await TryOperationAsync(operation, replicationDestination, primaryOperation, !operationResult.WasTimeout && hasMoreReplicationDestinations, token).ConfigureAwait(false);

                if (operationResult.Success)
                    return operationResult.Result;

                FailureCounters.IncrementFailureCount(replicationDestination.Url);
                if (!operationResult.WasTimeout && FailureCounters.IsFirstFailure(replicationDestination.Url))
                {
                    operationResult = await TryOperationAsync(operation, replicationDestination, primaryOperation, hasMoreReplicationDestinations, token).ConfigureAwait(false);

                    if (operationResult.Success)
                        return operationResult.Result;

                    FailureCounters.IncrementFailureCount(replicationDestination.Url);
                }
            }

            // this should not be thrown, but since I know the value of should...
            throw new InvalidOperationException(@"Attempted to connect to master and all replicas have failed, giving up.
There is a high probability of a network problem preventing access to all the replicas.
Failed to get in touch with any of the " + (1 + localReplicationDestinations.Count) + " Raven instances.");
        }

        protected async virtual Task<AsyncOperationResult<T>> TryOperationAsync<T>(Func<OperationMetadata, Task<T>> operation, OperationMetadata operationMetadata,
            OperationMetadata primaryOperationMetadata, bool avoidThrowing)
        {
            return await TryOperationAsync(operation, operationMetadata, primaryOperationMetadata, avoidThrowing, default(CancellationToken)).ConfigureAwait(false);
        }

        protected async virtual Task<AsyncOperationResult<T>> TryOperationAsync<T>(Func<OperationMetadata, Task<T>> operation, OperationMetadata operationMetadata, OperationMetadata primaryOperationMetadata, bool avoidThrowing, CancellationToken cancellationToken)
        {
            var tryWithPrimaryCredentials = FailureCounters.IsFirstFailure(operationMetadata.Url) && primaryOperationMetadata != null;

            bool shouldTryAgain = false;
            try
            {
                cancellationToken.ThrowCancellationIfNotDefault(); //canceling the task here potentially will stop the recursion
                
                var result = await operation(tryWithPrimaryCredentials ? new OperationMetadata(operationMetadata.Url, primaryOperationMetadata.Credentials, primaryOperationMetadata.ClusterInformation) : operationMetadata).ConfigureAwait(false);
                
                FailureCounters.ResetFailureCount(operationMetadata.Url);
                
                return new AsyncOperationResult<T>
                {
                    Result = result,
                    Success = true
                };
            }
            catch (Exception e)
            {
                ErrorResponseException errorResponseException;

                var ae = e as AggregateException;                
                if (ae != null)
                {
                    errorResponseException = ae.ExtractSingleInnerException() as ErrorResponseException;
                }
                else
                {
                    errorResponseException = e as ErrorResponseException;
                }

                if (tryWithPrimaryCredentials && operationMetadata.Credentials.HasCredentials() && errorResponseException != null)
                {
                    FailureCounters.IncrementFailureCount(operationMetadata.Url);

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
            return await TryOperationAsync(operation, operationMetadata, primaryOperationMetadata, avoidThrowing, cancellationToken).ConfigureAwait(false);
        }

        public virtual void Dispose()
        {
        }
        }

    /// <summary>
    /// The event arguments for when the failover status changed
    /// </summary>
    public class FailoverStatusChangedEventArgs : EventArgs
    {
        /// <summary>
        /// Whatever that url is now failing
        /// </summary>
        public bool Failing { get; set; }
        /// <summary>
        /// The url whose failover status changed
        /// </summary>
        public string Url { get; set; }
    }
}
