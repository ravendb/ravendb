//-----------------------------------------------------------------------
// <copyright file="ReplicationInformer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Client.Document;
using Raven.Imports.Newtonsoft.Json.Linq;

namespace Raven.Client.Connection
{
    using Raven.Abstractions.Connection;

	/// <summary>
	/// Replication and failover management on the client side
	/// </summary>
	public abstract class ReplicationInformerBase<TClient> : IReplicationInformerBase<TClient>
	{
		protected readonly ILog log = LogManager.GetCurrentClassLogger();

        protected bool firstTime = true;
		protected readonly Convention conventions;
		protected DateTime lastReplicationUpdate = DateTime.MinValue;
        protected readonly object replicationLock = new object();
		private static readonly List<OperationMetadata> Empty = new List<OperationMetadata>();
		protected static int readStripingBase;

		/// <summary>
		/// Notify when the failover status changed
		/// </summary>
		public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged = delegate { };

        public List<OperationMetadata> ReplicationDestinations { get; protected set; }

		/// <summary>
		/// Gets the replication destinations.
		/// </summary>
		/// <value>The replication destinations.</value>
		public List<OperationMetadata> ReplicationDestinationsUrls
		{
			get
			{
				if (conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
					return Empty;

				return ReplicationDestinations
					.Select(operationMetadata => new OperationMetadata(operationMetadata))
					.ToList();
			}
		}

		///<summary>
		/// Create a new instance of this class
		///</summary>
		protected ReplicationInformerBase(Convention conventions)
		{
			this.conventions = conventions;
            ReplicationDestinations = new List<OperationMetadata>();
		}

        /// <summary>
        /// Updates the replication information if needed.
        /// </summary>
        public abstract Task UpdateReplicationInformationIfNeeded(TClient client);

        /// <summary>
        /// Refreshes the replication information.
        /// Expert use only.
        /// </summary>
        public abstract void RefreshReplicationInformation(TClient client);

        protected abstract void UpdateReplicationInformationFromDocument(JsonDocument document);

		protected readonly System.Collections.Concurrent.ConcurrentDictionary<string, FailureCounter> failureCounts = new System.Collections.Concurrent.ConcurrentDictionary<string, FailureCounter>();

		protected Task refreshReplicationInformationTask;

		public class FailureCounter
		{
			public long Value;
			public DateTime LastCheck;
			public bool ForceCheck;

			public FailureCounter()
			{
				LastCheck = SystemTime.UtcNow;
			}
		}


		/// <summary>
		/// Get the current failure count for the url
		/// </summary>
		public long GetFailureCount(string operationUrl)
		{
			return GetHolder(operationUrl).Value;
		}

		/// <summary>
		/// Get failure last check time for the url
		/// </summary>
		public DateTime GetFailureLastCheck(string operationUrl)
		{
			return GetHolder(operationUrl).LastCheck;
		}

		/// <summary>
		/// Should execute the operation using the specified operation URL
		/// </summary>
		public virtual bool ShouldExecuteUsing(string operationUrl, int currentRequest, string method, bool primary)
		{
			if (primary == false)
				AssertValidOperation(method);

			var failureCounter = GetHolder(operationUrl);
			if (failureCounter.Value == 0 || failureCounter.ForceCheck)
			{
				failureCounter.LastCheck = SystemTime.UtcNow;
				return true;
			}

			if (currentRequest % GetCheckRepetitionRate(failureCounter.Value) == 0)
			{
				failureCounter.LastCheck = SystemTime.UtcNow;
				return true;
			}

			if ((SystemTime.UtcNow - failureCounter.LastCheck) > conventions.MaxFailoverCheckPeriod)
			{
				failureCounter.LastCheck = SystemTime.UtcNow;
				return true;
			}

			return false;
		}

		private int GetCheckRepetitionRate(long value)
		{
			if (value < 2)
				return (int)value;
			if (value < 10)
				return 2;
			if (value < 100)
				return 10;
			if (value < 1000)
				return 100;
			if (value < 10000)
				return 1000;
			if (value < 100000)
				return 10000;
			return 100000;
		}

		protected void AssertValidOperation(string method)
		{
			switch (conventions.FailoverBehaviorWithoutFlags)
			{
				case FailoverBehavior.AllowReadsFromSecondaries:
					if (method == "GET")
						return;
					break;
				case FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries:
					return;
				case FailoverBehavior.FailImmediately:
					var allowReadFromAllServers = conventions.FailoverBehavior.HasFlag(FailoverBehavior.ReadFromAllServers);
					if (allowReadFromAllServers && method == "GET")
						return;
					break;
			}
			throw new InvalidOperationException("Could not replicate " + method +
												" operation to secondary node, failover behavior is: " +
												conventions.FailoverBehavior);
		}

		protected FailureCounter GetHolder(string operationUrl)
		{
			return failureCounts.GetOrAdd(operationUrl, new FailureCounter());
		}

		/// <summary>
		/// Determines whether this is the first failure on the specified operation URL.
		/// </summary>
		/// <param name="operationUrl">The operation URL.</param>
		public bool IsFirstFailure(string operationUrl)
		{
			FailureCounter value = GetHolder(operationUrl);
			return value.Value == 0;
		}

		/// <summary>
		/// Increments the failure count for the specified operation URL
		/// </summary>
		/// <param name="operationUrl">The operation URL.</param>
		public void IncrementFailureCount(string operationUrl)
		{
			FailureCounter value = GetHolder(operationUrl);
			value.ForceCheck = false;
			var current = Interlocked.Increment(ref value.Value);
			if (current == 1)// first failure
			{
				FailoverStatusChanged(this, new FailoverStatusChangedEventArgs
				{
					Url = operationUrl,
					Failing = true
				});
			}
		}

        protected static bool IsInvalidDestinationsDocument(JsonDocument document)
		{
			return document == null ||
				   document.DataAsJson.ContainsKey("Destinations") == false ||
				   document.DataAsJson["Destinations"] == null ||
				   document.DataAsJson["Destinations"].Type == JTokenType.Null;
		}

		/// <summary>
		/// Resets the failure count for the specified URL
		/// </summary>
		/// <param name="operationUrl">The operation URL.</param>
		public virtual void ResetFailureCount(string operationUrl)
		{
			var value = GetHolder(operationUrl);
			var oldVal = Interlocked.Exchange(ref value.Value, 0);
			value.LastCheck = SystemTime.UtcNow;
			value.ForceCheck = false;
			if (oldVal != 0)
			{
				FailoverStatusChanged(this,
					new FailoverStatusChangedEventArgs
					{
						Url = operationUrl,
						Failing = false
					});
			}
		}

		public virtual int GetReadStripingBase()
		{
			return Interlocked.Increment(ref readStripingBase);
		}

		#region ExecuteWithReplication

		public virtual T ExecuteWithReplication<T>(string method, string primaryUrl, OperationCredentials primaryCredentials, int currentRequest, int currentReadStripingBase, Func<OperationMetadata, T> operation)
		{
			T result;
			var timeoutThrown = false;

			var localReplicationDestinations = ReplicationDestinationsUrls; // thread safe copy
			var primaryOperation = new OperationMetadata(primaryUrl, primaryCredentials);

			var shouldReadFromAllServers = conventions.FailoverBehavior.HasFlag(FailoverBehavior.ReadFromAllServers);
			if (shouldReadFromAllServers && method == "GET")
			{
				var replicationIndex = currentReadStripingBase % (localReplicationDestinations.Count + 1);
				// if replicationIndex == destinations count, then we want to use the master
				// if replicationIndex < 0, then we were explicitly instructed to use the master
				if (replicationIndex < localReplicationDestinations.Count && replicationIndex >= 0)
				{
					// if it is failing, ignore that, and move to the master or any of the replicas
					if (ShouldExecuteUsing(localReplicationDestinations[replicationIndex].Url, currentRequest, method, false))
					{
						if (TryOperation(operation, localReplicationDestinations[replicationIndex], primaryOperation, true, out result, out timeoutThrown))
							return result;
					}
				}
			}

			if (ShouldExecuteUsing(primaryOperation.Url, currentRequest, method, true))
			{
				if (TryOperation(operation, primaryOperation, null, !timeoutThrown && localReplicationDestinations.Count > 0, out result, out timeoutThrown))
					return result;
				if (!timeoutThrown && IsFirstFailure(primaryOperation.Url) &&
					TryOperation(operation, primaryOperation, null, localReplicationDestinations.Count > 0, out result, out timeoutThrown))
					return result;
				IncrementFailureCount(primaryOperation.Url);
			}

			for (var i = 0; i < localReplicationDestinations.Count; i++)
			{
				var replicationDestination = localReplicationDestinations[i];
				if (ShouldExecuteUsing(replicationDestination.Url, currentRequest, method, false) == false)
					continue;
				if (TryOperation(operation, replicationDestination, primaryOperation, !timeoutThrown, out result, out timeoutThrown))
					return result;
				if (!timeoutThrown && IsFirstFailure(replicationDestination.Url) &&
					TryOperation(operation, replicationDestination, primaryOperation, localReplicationDestinations.Count > i + 1, out result,
								 out timeoutThrown))
					return result;
				IncrementFailureCount(replicationDestination.Url);
			}
			// this should not be thrown, but since I know the value of should...
			throw new InvalidOperationException(@"Attempted to connect to master and all replicas have failed, giving up.
There is a high probability of a network problem preventing access to all the replicas.
Failed to get in touch with any of the " + (1 + localReplicationDestinations.Count) + " Raven instances.");
		}
		#endregion

		
		public async Task<T> ExecuteWithReplicationAsync<T>(string method, string primaryUrl, OperationCredentials primaryCredentials, int currentRequest, int currentReadStripingBase, Func<OperationMetadata, Task<T>> operation)
		{
			var timeoutThrown = false;

			var localReplicationDestinations = ReplicationDestinationsUrls; // thread safe copy
			var primaryOperation = new OperationMetadata(primaryUrl, primaryCredentials);

			var shouldReadFromAllServers = conventions.FailoverBehavior.HasFlag(FailoverBehavior.ReadFromAllServers);
			if (shouldReadFromAllServers && method == "GET")
			{
				var replicationIndex = currentReadStripingBase % (localReplicationDestinations.Count + 1);
				// if replicationIndex == destinations count, then we want to use the master
				// if replicationIndex < 0, then we were explicitly instructed to use the master
				if (replicationIndex < localReplicationDestinations.Count && replicationIndex >= 0)
				{
					// if it is failing, ignore that, and move to the master or any of the replicas
					if (ShouldExecuteUsing(localReplicationDestinations[replicationIndex].Url, currentRequest, method, false))
					{
						var tuple = await TryOperationAsync(operation, localReplicationDestinations[replicationIndex], primaryOperation, true);
						if (tuple.Success)
							return tuple.Result;
						timeoutThrown = tuple.WasTimeout;
					}
				}
			}

			if (ShouldExecuteUsing(primaryOperation.Url, currentRequest, method, true))
			{
				var tuple = await TryOperationAsync(operation, primaryOperation, null, !timeoutThrown && localReplicationDestinations.Count > 0);
				if (tuple.Success)
					return tuple.Result;
				timeoutThrown = tuple.WasTimeout;

				if (!timeoutThrown && IsFirstFailure(primaryOperation.Url))
				{
					tuple = await TryOperationAsync(operation, primaryOperation, null, localReplicationDestinations.Count > 0);
					if (tuple.Success)
						return tuple.Result;
					timeoutThrown = tuple.WasTimeout;
				}
				IncrementFailureCount(primaryOperation.Url);
			}

			for (var i = 0; i < localReplicationDestinations.Count; i++)
			{
				var replicationDestination = localReplicationDestinations[i];
				if (ShouldExecuteUsing(replicationDestination.Url, currentRequest, method, false) == false)
					continue;

				var tuple = await TryOperationAsync(operation, replicationDestination, primaryOperation, !timeoutThrown);
				if (tuple.Success)
					return tuple.Result;
				timeoutThrown = tuple.WasTimeout;

				if (!timeoutThrown && IsFirstFailure(replicationDestination.Url))
				{
					tuple = await TryOperationAsync(operation, replicationDestination, primaryOperation, localReplicationDestinations.Count > i + 1);
					if (tuple.Success)
						return tuple.Result;
					timeoutThrown = tuple.WasTimeout;
				}
				IncrementFailureCount(replicationDestination.Url);
			}
			// this should not be thrown, but since I know the value of should...
			throw new InvalidOperationException(@"Attempted to connect to master and all replicas have failed, giving up.
There is a high probability of a network problem preventing access to all the replicas.
Failed to get in touch with any of the " + (1 + localReplicationDestinations.Count) + " Raven instances.");
		}

	    protected class AsyncOperationResult<T>
	    {
	        public T Result;
	        public bool WasTimeout;
	        public bool Success;
	    }

        protected async virtual Task<AsyncOperationResult<T>> TryOperationAsync<T>(Func<OperationMetadata, Task<T>> operation, OperationMetadata operationMetadata, OperationMetadata primaryOperationMetadata, bool avoidThrowing)
        {
            var tryWithPrimaryCredentials = IsFirstFailure(operationMetadata.Url) && primaryOperationMetadata != null;
            bool shouldTryAgain = false;

            try
            {

                var result = await operation(tryWithPrimaryCredentials ? new OperationMetadata(operationMetadata.Url, primaryOperationMetadata.Credentials) : operationMetadata);
                ResetFailureCount(operationMetadata.Url);
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
                if (tryWithPrimaryCredentials && operationMetadata.Credentials.HasCredentials() && errorResponseException != null)
                {
                    IncrementFailureCount(operationMetadata.Url);

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
                    if (IsServerDown(e, out wasTimeout))
                    {
                        return new AsyncOperationResult<T>
                        {
                            Success = false,
                            WasTimeout = wasTimeout
                        };
                    }
                    throw;
                }
            }
            return await TryOperationAsync(operation, operationMetadata, primaryOperationMetadata, avoidThrowing);
        }

		protected virtual bool TryOperation<T>(Func<OperationMetadata, T> operation, OperationMetadata operationMetadata, OperationMetadata primaryOperationMetadata, bool avoidThrowing, out T result, out bool wasTimeout)
		{
			var tryWithPrimaryCredentials = IsFirstFailure(operationMetadata.Url) && primaryOperationMetadata != null;

			try
			{
				
				result = operation(tryWithPrimaryCredentials ? new OperationMetadata(operationMetadata.Url, primaryOperationMetadata.Credentials) : operationMetadata);
				ResetFailureCount(operationMetadata.Url);
				wasTimeout = false;
				return true;
			}
			catch (Exception e)
			{
				var webException = e as WebException;
				if (tryWithPrimaryCredentials && operationMetadata.Credentials.HasCredentials() && webException != null)
				{
					IncrementFailureCount(operationMetadata.Url);

					var response = webException.Response as HttpWebResponse;
					if (response != null && response.StatusCode == HttpStatusCode.Unauthorized)
					{
						return TryOperation(operation, operationMetadata, primaryOperationMetadata, avoidThrowing, out result, out wasTimeout);
					}
				}

				if (avoidThrowing == false)
					throw;
				result = default(T);

				if (IsServerDown(e, out wasTimeout))
				{
					return false;
				}
				throw;
			}
		}

		protected class ExecuteWithReplicationState<T>
		{
			public ExecuteWithReplicationState(string method, string primaryUrl, OperationCredentials primaryCredentials, int currentRequest, int readStripingBase, Func<OperationMetadata, Task<T>> operation)
			{
				Method = method;
				PrimaryUrl = primaryUrl;
				CurrentRequest = currentRequest;
				ReadStripingBase = readStripingBase;
				Operation = operation;
				PrimaryCredentials = primaryCredentials;

				State = ExecuteWithReplicationStates.Start;
			}

			public readonly string Method;
			public readonly Func<OperationMetadata, Task<T>> Operation;
			public readonly string PrimaryUrl;
			public readonly int CurrentRequest;
			public readonly int ReadStripingBase;
			public readonly OperationCredentials PrimaryCredentials;

			public ExecuteWithReplicationStates State = ExecuteWithReplicationStates.Start;
			public int LastAttempt = -1;
			public List<OperationMetadata> ReplicationDestinations;
			public bool TimeoutThrown;

			public ExecuteWithReplicationState<T> With(ExecuteWithReplicationStates state)
			{
				State = state;
				return this;
			}
		}

		protected enum ExecuteWithReplicationStates
		{
			Start,
			AfterTryingWithStripedServer,
			AfterTryingWithDefaultUrl,
			TryAllServers,
			AfterTryingAllServers,
			TryAllServersSecondAttempt,
			TryAllServersFailedTwice,
			AfterTryingWithDefaultUrlTwice
		}

		public bool IsHttpStatus(Exception e, params HttpStatusCode[] httpStatusCode)
		{
			var aggregateException = e as AggregateException;
			if (aggregateException != null)
			{
				e = aggregateException.ExtractSingleInnerException();
			}

			var ere = e as ErrorResponseException ?? e.InnerException as ErrorResponseException;
		    if (ere != null)
		    {
			    return httpStatusCode.Contains(ere.StatusCode);
		    }
			var webException = (e as WebException) ?? (e.InnerException as WebException);
			if (webException != null)
			{
				var httpWebResponse = webException.Response as HttpWebResponse;
				if (httpWebResponse != null && httpStatusCode.Contains(httpWebResponse.StatusCode))
					return true;
			}

			return false;
		}

		public virtual bool IsServerDown(Exception e, out bool timeout)
		{
			timeout = false;

			var aggregateException = e as AggregateException;
			if (aggregateException != null)
			{
				e = aggregateException.ExtractSingleInnerException();
			}

		    var ere = e as ErrorResponseException ?? e.InnerException as ErrorResponseException;
		    if (ere != null)
		    {
		        if (IsServerDown(ere.StatusCode, out timeout))
		            return true;
		    }

			var webException = (e as WebException) ?? (e.InnerException as WebException);
			if (webException != null)
			{
				switch (webException.Status)
				{
#if !NETFX_CORE
					case WebExceptionStatus.Timeout:
						timeout = true;
						return true;
					case WebExceptionStatus.NameResolutionFailure:
					case WebExceptionStatus.ReceiveFailure:
					case WebExceptionStatus.PipelineFailure:
					case WebExceptionStatus.ConnectionClosed:

#endif
					case WebExceptionStatus.ConnectFailure:
					case WebExceptionStatus.SendFailure:
						return true;
				}

				var httpWebResponse = webException.Response as HttpWebResponse;
				if (httpWebResponse != null)
				{
					if (IsServerDown(httpWebResponse.StatusCode, out timeout))
                        return true;
				}
			}
			return
#if !NETFX_CORE
 e.InnerException is SocketException ||
#endif
 e.InnerException is IOException;
		}

        private static bool IsServerDown(HttpStatusCode httpStatusCode, out bool timeout)
	    {
            timeout = false;
            switch (httpStatusCode)
	        {
	            case HttpStatusCode.RequestTimeout:
	            case HttpStatusCode.GatewayTimeout:
	                timeout = true;
	                return true;
	            case HttpStatusCode.BadGateway:
	            case HttpStatusCode.ServiceUnavailable:
	                return true;
	        }
	        return false;
	    }

	    public virtual void Dispose()
		{
			var replicationInformationTaskCopy = refreshReplicationInformationTask;
			if (replicationInformationTaskCopy != null)
				replicationInformationTaskCopy.Wait();
		}

		public void ForceCheck(string primaryUrl, bool shouldForceCheck)
		{
			var failureCounter = this.GetHolder(primaryUrl);
			failureCounter.ForceCheck = shouldForceCheck;
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
