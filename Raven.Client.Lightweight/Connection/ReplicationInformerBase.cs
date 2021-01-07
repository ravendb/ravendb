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
using Raven.Client.Connection.Request;
using Raven.Client.Metrics;
using Raven.Imports.Newtonsoft.Json.Linq;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Connection;
using Raven.Client.Util;

namespace Raven.Client.Connection
{
    /// <summary>
    /// Replication and failover management on the client side
    /// </summary>
    public abstract class ReplicationInformerBase<TClient> : IReplicationInformerBase<TClient>
    {
#if !DNXCORE50
        protected readonly ILog Log = LogManager.GetCurrentClassLogger();
#else
        protected readonly ILog Log = LogManager.GetLogger(typeof(ReplicationInformerBase<TClient>));
#endif

        protected readonly QueryConvention Conventions;

        private readonly HttpJsonRequestFactory requestFactory;

        private readonly Func<string, IRequestTimeMetric> requestTimeMetricGetter;

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

        public int DelayTimeInMiliSec { get; set; }

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

        protected ReplicationInformerBase(QueryConvention conventions, HttpJsonRequestFactory requestFactory, Func<string, IRequestTimeMetric> requestTimeMetricGetter, int delayTime = 1000)
        {
            Conventions = conventions;
            this.requestFactory = requestFactory;
            this.requestTimeMetricGetter = requestTimeMetricGetter;
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

        public abstract void UpdateReplicationInformationFromDocument(JsonDocument document);

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
            if ((currentTask.IsCompleted || currentTask.IsFaulted || currentTask.IsCanceled) && DelayTimeInMiliSec > 0)
            {
                var tcs = TaskCompletionSourceFactory.Create<object>();
                var old = Interlocked.CompareExchange(ref failureCounter.CheckDestination, tcs.Task, currentTask);
                if (old == currentTask)
                {
                    if (currentTask.IsCanceled || currentTask.IsFaulted)
                        GC.KeepAlive(currentTask.Exception); // observer & ignore this exception

                    CheckIfServerIsUpNow(operationMetadata, primaryOperation, token)
                        .ContinueWith(task =>
                        {
                            switch (task.Status)
                            {
                                case TaskStatus.RanToCompletion:
                                    tcs.TrySetResult(null);
                                    break;
                                case TaskStatus.Canceled:
                                    tcs.TrySetCanceled();
                                    break;
                                case TaskStatus.Faulted:
                                    if (task.Exception != null)
                                        tcs.TrySetException(task.Exception);
                                    else
                                        goto default;
                                    break;
                                default:
                                    tcs.TrySetCanceled();
                                    break;
                            }
                        }, token, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
                }
            }

            return false;
        }

        private async Task CheckIfServerIsUpNow(OperationMetadata operationMetadata, OperationMetadata primaryOperation, CancellationToken token)
        {
            for (int i = 0; i < 5; i++)
            {
                token.ThrowCancellationIfNotDefault();
                try
                {
                    var r = await TryOperationAsync<object>(async (metadata, requestTimeMetric) =>
                    {
                        var requestParams = new CreateHttpJsonRequestParams(null, GetServerCheckUrl(metadata.Url), HttpMethods.Get, metadata.Credentials, Conventions);
                        using (var request = requestFactory.CreateHttpJsonRequest(requestParams))
                        {
                            await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                        }
                        return null;
                    }, operationMetadata, primaryOperation, null, true, token).ConfigureAwait(false);
                    if (r.Success)
                    {
                        FailureCounters.ResetFailureCount(operationMetadata.Url);
                        return;
                    }
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
                await Task.Delay(DelayTimeInMiliSec, token).ConfigureAwait(false);
            }
        }

        protected abstract string GetServerCheckUrl(string baseUrl);

        protected void AssertValidOperation(HttpMethod method, Exception e)
        {
            switch (Conventions.FailoverBehaviorWithoutFlags)
            {
                case FailoverBehavior.AllowReadsFromSecondaries:
                case FailoverBehavior.AllowReadFromSecondariesWhenRequestTimeSlaThresholdIsReached:
                    if (method == HttpMethods.Get || method == HttpMethods.Head)
                        return;
                    break;
                case FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries:
                    return;
                case FailoverBehavior.FailImmediately:
                    var allowReadFromAllServers = Conventions.FailoverBehavior.HasFlag(FailoverBehavior.ReadFromAllServers);
                    if (allowReadFromAllServers && (method == HttpMethods.Get || method == HttpMethods.Head))
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
            int currentRequest,
            int currentReadStripingBase,
            Func<OperationMetadata, IRequestTimeMetric, Task<T>> operation,
            CancellationToken token = default(CancellationToken))
        {
            Debug.Assert(typeof(T).FullName.Contains("Task") == false);

            var localReplicationDestinations = ReplicationDestinationsUrls; // thread safe copy
            var primaryOperation = new OperationMetadata(primaryUrl, primaryCredentials, null);

            var operationResult = new AsyncOperationResult<T>();
            var shouldReadFromAllServers = Conventions.FailoverBehavior.HasFlag(FailoverBehavior.ReadFromAllServers);

            var allowReadFromSecondariesWhenRequestTimeSlaThresholdIsPassed = Conventions.FailoverBehavior.HasFlag(FailoverBehavior.AllowReadFromSecondariesWhenRequestTimeSlaThresholdIsReached);
            var primaryRequestTimeMetric = requestTimeMetricGetter?.Invoke(primaryOperation.Url);
            var complexTimeMetric = new ComplexTimeMetric();

            if (method == HttpMethods.Get && (shouldReadFromAllServers || allowReadFromSecondariesWhenRequestTimeSlaThresholdIsPassed))
            {
                var replicationIndex = -1;
                if (allowReadFromSecondariesWhenRequestTimeSlaThresholdIsPassed && shouldReadFromAllServers)
                {
                    if (requestTimeMetricGetter != null && primaryRequestTimeMetric != null)
                    {
                        complexTimeMetric.AddCurrent(primaryRequestTimeMetric); // want to decrease everything
                        foreach (var destination in localReplicationDestinations)
                            complexTimeMetric.AddCurrent(requestTimeMetricGetter(destination.Url));

                        replicationIndex = currentReadStripingBase % (localReplicationDestinations.Count + 1); // include primary
                        for (var i = 0; i < localReplicationDestinations.Count + 1; i++)
                        {
                            IRequestTimeMetric metric;
                            if (replicationIndex >= localReplicationDestinations.Count) // primary
                                metric = primaryRequestTimeMetric;
                            else
                                metric = requestTimeMetricGetter(localReplicationDestinations[replicationIndex].Url);

                            if (metric.RateSurpassed(Conventions) == false)
                            {
                                complexTimeMetric.AddCurrent(metric);
                                break;
                            }

                            replicationIndex = (replicationIndex + 1) % (localReplicationDestinations.Count + 1);
                        }
                    }
                }
                else if (allowReadFromSecondariesWhenRequestTimeSlaThresholdIsPassed)
                {
                    if (primaryRequestTimeMetric != null)
                    {
                        complexTimeMetric.AddCurrent(primaryRequestTimeMetric);

                        if (complexTimeMetric.RateSurpassed(Conventions))
                            replicationIndex = currentReadStripingBase % localReplicationDestinations.Count; // this will skip the primary
                    }
                }
                else if (shouldReadFromAllServers)
                {
                    replicationIndex = currentReadStripingBase % (localReplicationDestinations.Count + 1);
                }

                // if replicationIndex == destinations count, then we want to use the master
                // if replicationIndex < 0, then we were explicitly instructed to use the master
                if (replicationIndex < localReplicationDestinations.Count && replicationIndex >= 0)
                {
                    var destination = localReplicationDestinations[replicationIndex];
                    // if it is failing, ignore that, and move to the master or any of the replicas
                    if (ShouldExecuteUsing(destination, primaryOperation, method, false, null, token))
                    {
                        if (requestTimeMetricGetter != null)
                            complexTimeMetric.AddCurrent(requestTimeMetricGetter(destination.Url));

                        operationResult = await TryOperationAsync(operation, destination, primaryOperation, complexTimeMetric, true, token).ConfigureAwait(false);
                        if (operationResult.Success)
                            return operationResult.Result;
                    }
                }
            }

            if (ShouldExecuteUsing(primaryOperation, primaryOperation, method, true, null, token))
            {
                if (primaryRequestTimeMetric != null)
                    complexTimeMetric.AddCurrent(primaryRequestTimeMetric);

                operationResult = await TryOperationAsync(operation, primaryOperation, null, complexTimeMetric, !operationResult.WasTimeout && localReplicationDestinations.Count > 0, token)
                    .ConfigureAwait(false);

                if (operationResult.Success)
                    return operationResult.Result;

                FailureCounters.IncrementFailureCount(primaryOperation.Url);
                if (operationResult.WasTimeout == false && FailureCounters.IsFirstFailure(primaryOperation.Url))
                {
                    operationResult = await TryOperationAsync(operation, primaryOperation, null, complexTimeMetric, localReplicationDestinations.Count > 0, token).ConfigureAwait(false);

                    if (operationResult.Success)
                        return operationResult.Result;

                    FailureCounters.IncrementFailureCount(primaryOperation.Url);
                }
            }

            for (var i = 0; i < localReplicationDestinations.Count; i++)
            {
                token.ThrowCancellationIfNotDefault();

                var destination = localReplicationDestinations[i];
                if (ShouldExecuteUsing(destination, primaryOperation, method, false, operationResult.Error, token) == false)
                    continue;

                if (requestTimeMetricGetter != null)
                    complexTimeMetric.AddCurrent(requestTimeMetricGetter(destination.Url));

                var hasMoreReplicationDestinations = localReplicationDestinations.Count > i + 1;
                operationResult = await TryOperationAsync(operation, destination, primaryOperation, complexTimeMetric, !operationResult.WasTimeout && hasMoreReplicationDestinations, token).ConfigureAwait(false);

                if (operationResult.Success)
                    return operationResult.Result;

                FailureCounters.IncrementFailureCount(destination.Url);
                if (operationResult.WasTimeout == false && FailureCounters.IsFirstFailure(destination.Url))
                {
                    operationResult = await TryOperationAsync(operation, destination, primaryOperation, complexTimeMetric, hasMoreReplicationDestinations, token).ConfigureAwait(false);

                    // tuple = await TryOperationAsync(operation, replicationDestination, primaryOperation, localReplicationDestinations.Count > i + 1).ConfigureAwait(false);
                    if (operationResult.Success)
                        return operationResult.Result;
                    FailureCounters.IncrementFailureCount(destination.Url);
                }
            }

            // this should not be thrown, but since I know the value of should...
            throw new InvalidOperationException(@"Attempted to connect to master and all replicas have failed, giving up.
There is a high probability of a network problem preventing access to all the replicas.
Failed to get in touch with any of the " + (1 + localReplicationDestinations.Count) + " Raven instances.");
        }

        protected virtual async Task<AsyncOperationResult<T>> TryOperationAsync<T>(Func<OperationMetadata, IRequestTimeMetric, Task<T>> operation, OperationMetadata operationMetadata,
            OperationMetadata primaryOperationMetadata, IRequestTimeMetric requestTimeMetric, bool avoidThrowing, CancellationToken cancellationToken = default(CancellationToken))
        {
            var tryWithPrimaryCredentials = FailureCounters.IsFirstFailure(operationMetadata.Url) && primaryOperationMetadata != null;
            bool shouldTryAgain = false;

            try
            {
                cancellationToken.ThrowCancellationIfNotDefault(); //canceling the task here potentially will stop the recursion
                var result = await operation(tryWithPrimaryCredentials ? new OperationMetadata(operationMetadata.Url, primaryOperationMetadata.Credentials, primaryOperationMetadata.ClusterInformation) : operationMetadata, requestTimeMetric).ConfigureAwait(false);
                FailureCounters.ResetFailureCount(operationMetadata.Url);
                return new AsyncOperationResult<T>
                {
                    Result = result,
                    Success = true
                };
            }
            catch (Exception e)
            {
                var ae = e as AggregateException;
                var singleInnerException = ae.ExtractSingleInnerException();
                ErrorResponseException errorResponseException;
                if (ae != null)
                {
                    errorResponseException = singleInnerException as ErrorResponseException;
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
                    {
                        var timeoutException = singleInnerException as TimeoutException;
                        if (timeoutException != null)
                            throw timeoutException;

                        throw;
                    }

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

                    if (errorResponseException != null)
                        throw errorResponseException;

                    throw;
                }
            }
            return await TryOperationAsync(operation, operationMetadata, primaryOperationMetadata, requestTimeMetric, avoidThrowing, cancellationToken).ConfigureAwait(false);
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
