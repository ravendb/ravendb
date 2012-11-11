//-----------------------------------------------------------------------
// <copyright file="ReplicationInformer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.IsolatedStorage;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using System.Net;
using System.Net.Sockets;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Client.Extensions;

#if SILVERLIGHT
using Raven.Client.Silverlight.Connection.Async;
using Raven.Client.Silverlight.MissingFromSilverlight;
using Raven.Json.Linq;
#endif

namespace Raven.Client.Connection
{


	/// <summary>
	/// Replication and failover management on the client side
	/// </summary>
	public class ReplicationInformer : IDisposable
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private bool firstTime = true;
		protected readonly DocumentConvention conventions;
		private const string RavenReplicationDestinations = "Raven/Replication/Destinations";
		protected DateTime lastReplicationUpdate = DateTime.MinValue;
		private readonly object replicationLock = new object();
		private List<ReplicationDestinationData> replicationDestinations = new List<ReplicationDestinationData>();
		private static readonly List<string> Empty = new List<string>();
		protected static int readStripingBase;

		/// <summary>
		/// Notify when the failover status changed
		/// </summary>
		public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged = delegate { };
		
		public List<ReplicationDestinationData> ReplicationDestinations
		{
			get { return replicationDestinations; }
		}

		/// <summary>
		/// Gets the replication destinations.
		/// </summary>
		/// <value>The replication destinations.</value>
		public List<string> ReplicationDestinationsUrls
		{
			get
			{
				if (conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
					return Empty;

				return replicationDestinations.Select(replicationDestinationData => replicationDestinationData.Url).ToList();
			}
		}

		///<summary>
		/// Create a new instance of this class
		///</summary>
		public ReplicationInformer(DocumentConvention conventions)
		{
			this.conventions = conventions;
		}

#if !SILVERLIGHT
		private readonly System.Collections.Concurrent.ConcurrentDictionary<string, FailureCounter> failureCounts = new System.Collections.Concurrent.ConcurrentDictionary<string, FailureCounter>();
#else
		private readonly Dictionary<string, FailureCounter> failureCounts = new Dictionary<string, FailureCounter>();
#endif

		private Task refreshReplicationInformationTask;

		/// <summary>
		/// Updates the replication information if needed.
		/// </summary>
		/// <param name="serverClient">The server client.</param>
#if SILVERLIGHT
		public Task UpdateReplicationInformationIfNeeded(AsyncServerClient serverClient)
#else
		public Task UpdateReplicationInformationIfNeeded(ServerClient serverClient)
#endif
		{
			if (conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
				return new CompletedTask();

			if (lastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
				return new CompletedTask();

			lock (replicationLock)
			{
				if (firstTime)
				{
					var serverHash = GetServerHash(serverClient);

					var document = TryLoadReplicationInformationFromLocalCache(serverHash);
					if (IsInvalidDestinationsDocument(document) == false)
					{
						UpdateReplicationInformationFromDocument(document);
					}
				}

				firstTime = false;

				if (lastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
					return new CompletedTask();

				var taskCopy = refreshReplicationInformationTask;
				if (taskCopy != null)
					return taskCopy;

				return refreshReplicationInformationTask = Task.Factory.StartNew(() => RefreshReplicationInformation(serverClient))
					.ContinueWith(task =>
					{
						if (task.Exception != null)
						{
							log.ErrorException("Failed to refresh replication information", task.Exception);
						}
						refreshReplicationInformationTask = null;
					});
			}
		}

		private class FailureCounter
		{
			public int Value;
			public DateTime LastCheck;

			public FailureCounter()
			{
				LastCheck = SystemTime.UtcNow;
			}
		}


		/// <summary>
		/// Get the current failure count for the url
		/// </summary>
		public int GetFailureCount(string operationUrl)
		{
			return GetHolder(operationUrl).Value;
		}

		/// <summary>
		/// Should execute the operation using the specified operation URL
		/// </summary>
		public virtual bool ShouldExecuteUsing(string operationUrl, int currentRequest, string method, bool primary)
		{
			if (primary == false)
				AssertValidOperation(method);

			var failureCounter = GetHolder(operationUrl);
			if (failureCounter.Value == 0)
				return true;


			if (currentRequest % GetCheckReptitionRate(failureCounter.Value) == 0)
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

		private int GetCheckReptitionRate(int value)
		{
			if (value < 2)
				return value;
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
					var allowReadFromAllServers = (conventions.FailoverBehavior & FailoverBehavior.ReadFromAllServers) ==
												  FailoverBehavior.ReadFromAllServers;
					if (allowReadFromAllServers && method == "GET")
						return;
					break;
			}
			throw new InvalidOperationException("Could not replicate " + method +
												" operation to secondary node, failover behavior is: " +
												conventions.FailoverBehavior);
		}

		private FailureCounter GetHolder(string operationUrl)
		{
#if !SILVERLIGHT
			return failureCounts.GetOrAdd(operationUrl, new FailureCounter());
#else
			// need to compensate for 3.5 not having concnurrent dic.

			FailureCounter value;
			if (failureCounts.TryGetValue(operationUrl, out value) == false)
			{
				lock (replicationLock)
				{
					if (failureCounts.TryGetValue(operationUrl, out value) == false)
					{
						failureCounts[operationUrl] = value = new FailureCounter();
					}
				}
			}
			return value;
#endif

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

		private static bool IsInvalidDestinationsDocument(JsonDocument document)
		{
			return document == null ||
				   document.DataAsJson.ContainsKey("Destinations") == false ||
				   document.DataAsJson["Destinations"] == null ||
				   document.DataAsJson["Destinations"].Type == JTokenType.Null;
		}

		/// <summary>
		/// Refreshes the replication information.
		/// Expert use only.
		/// </summary>
		[MethodImpl(MethodImplOptions.Synchronized)]
#if SILVERLIGHT
		public Task RefreshReplicationInformation(AsyncServerClient commands)
		{
			var serverHash = GetServerHash(commands);
			return commands.DirectGetAsync(commands.Url, RavenReplicationDestinations).ContinueWith((Task<JsonDocument> getTask) =>
			{
				JsonDocument document;
				if (getTask.Status == TaskStatus.RanToCompletion)
				{
					document = getTask.Result;
					failureCounts[commands.Url] = new FailureCounter(); // we just hit the master, so we can reset its failure count
				}
				else
				{
					log.ErrorException("Could not contact master for new replication information", getTask.Exception);
					document = TryLoadReplicationInformationFromLocalCache(serverHash);
				}


				if (IsInvalidDestinationsDocument(document))
				{
					lastReplicationUpdate = SystemTime.UtcNow; // checked and not found
					return;
				}

				TrySavingReplicationInformationToLocalCache(serverHash, document);

				UpdateReplicationInformationFromDocument(document);

				lastReplicationUpdate = SystemTime.UtcNow;
			});
		}

		
#else
		public void RefreshReplicationInformation(ServerClient commands)
		{
			var serverHash = GetServerHash(commands);

			JsonDocument document;
			try
			{
				document = commands.DirectGet(commands.Url, RavenReplicationDestinations);
				failureCounts[commands.Url] = new FailureCounter(); // we just hit the master, so we can reset its failure count
			}
			catch (Exception e)
			{
				log.ErrorException("Could not contact master for new replication information", e);
				document = TryLoadReplicationInformationFromLocalCache(serverHash);
			}
			if (document == null)
			{
				lastReplicationUpdate = SystemTime.UtcNow; // checked and not found
				return;
			}

			TrySavingReplicationInformationToLocalCache(serverHash, document);

			UpdateReplicationInformationFromDocument(document);

			lastReplicationUpdate = SystemTime.UtcNow;
		}
#endif

		private void UpdateReplicationInformationFromDocument(JsonDocument document)
		{
			var replicationDocument = document.DataAsJson.JsonDeserialization<ReplicationDocument>();
			replicationDestinations = replicationDocument.Destinations.Select(x =>
			{
				var url = string.IsNullOrEmpty(x.ClientVisibleUrl) ? x.Url : x.ClientVisibleUrl;
				if (string.IsNullOrEmpty(url) || x.Disabled || x.IgnoredClient)
					return null;
				if (string.IsNullOrEmpty(x.Database))
					return new ReplicationDestinationData
					{
						Url = url,
					};
				return new ReplicationDestinationData
				{
					Url = MultiDatabase.GetRootDatabaseUrl(x.Url) + "/databases/" + x.Database + "/",
				};
			})
				// filter out replication destination that don't have the url setup, we don't know how to reach them
				// so we might as well ignore them. Probably private replication destination (using connection string names only)
				.Where(x => x != null)
				.ToList();
			foreach (var replicationDestination in replicationDestinations)
			{
				FailureCounter value;
				if (failureCounts.TryGetValue(replicationDestination.Url, out value))
					continue;
				failureCounts[replicationDestination.Url] = new FailureCounter();
			}
		}

		private IsolatedStorageFile GetIsolatedStorageFileForReplicationInformation()
		{
#if SILVERLIGHT
			return IsolatedStorageFile.GetUserStoreForSite();
#else
			return IsolatedStorageFile.GetMachineStoreForDomain();
#endif
		}

		private JsonDocument TryLoadReplicationInformationFromLocalCache(string serverHash)
		{
			try
			{
				using (var machineStoreForApplication = GetIsolatedStorageFileForReplicationInformation())
				{
					var path = "RavenDB Replication Information For - " + serverHash;

					if (machineStoreForApplication.GetFileNames(path).Length == 0)
						return null;

					using (var stream = new IsolatedStorageFileStream(path, FileMode.Open, machineStoreForApplication))
					{
						return stream.ToJObject().ToJsonDocument();
					}
				}
			}
			catch (Exception e)
			{
				log.ErrorException("Could not understand the persisted replication information", e);
				return null;
			}
		}

		private void TrySavingReplicationInformationToLocalCache(string serverHash, JsonDocument document)
		{
			try
			{
				using (var machineStoreForApplication = GetIsolatedStorageFileForReplicationInformation())
				{
					var path = "RavenDB Replication Information For - " + serverHash;
					using (var stream = new IsolatedStorageFileStream(path, FileMode.Create, machineStoreForApplication))
					{
						document.ToJson().WriteTo(stream);
					}
				}
			}
			catch (Exception e)
			{
				log.ErrorException("Could not persist the replication information", e);
			}
		}

#if SILVERLIGHT
		private static string GetServerHash(AsyncServerClient commands)
		{
			return BitConverter.ToString(MD5Core.GetHash(Encoding.UTF8.GetBytes(commands.Url)));
		}
#else
		private static string GetServerHash(ServerClient commands)
		{
			using (var md5 = MD5.Create())
			{
				return BitConverter.ToString(md5.ComputeHash(Encoding.UTF8.GetBytes(commands.Url)));
			}
		}
#endif

		/// <summary>
		/// Resets the failure count for the specified URL
		/// </summary>
		/// <param name="operationUrl">The operation URL.</param>
		public virtual void ResetFailureCount(string operationUrl)
		{
			var value = GetHolder(operationUrl);
			var oldVal = Interlocked.Exchange(ref value.Value, 0);
			value.LastCheck = SystemTime.UtcNow;
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

		public virtual T ExecuteWithReplication<T>(string method, string primaryUrl, int currentRequest, int currentReadStripingBase, Func<string, T> operation)
		{
			T result;
			var localReplicationDestinations = ReplicationDestinationsUrls; // thread safe copy

			var shouldReadFromAllServers = ((conventions.FailoverBehavior & FailoverBehavior.ReadFromAllServers) == FailoverBehavior.ReadFromAllServers);
			if (shouldReadFromAllServers && method == "GET")
			{
				var replicationIndex = currentReadStripingBase % (localReplicationDestinations.Count + 1);
				// if replicationIndex == destinations count, then we want to use the master
				// if replicationIndex < 0, then we were explicitly instructed to use the master
				if (replicationIndex < localReplicationDestinations.Count && replicationIndex >= 0)
				{
					// if it is failing, ignore that, and move to the master or any of the replicas
					if (ShouldExecuteUsing(localReplicationDestinations[replicationIndex], currentRequest, method, false))
					{
						if (TryOperation(operation, localReplicationDestinations[replicationIndex], true, out result))
							return result;
					}
				}
			}

			if (ShouldExecuteUsing(primaryUrl, currentRequest, method, true))
			{
				if (TryOperation(operation, primaryUrl, true, out result))
					return result;
				if (IsFirstFailure(primaryUrl) && TryOperation(operation, primaryUrl, localReplicationDestinations.Count > 0, out result))
					return result;
				IncrementFailureCount(primaryUrl);
			}

			for (var i = 0; i < localReplicationDestinations.Count; i++)
			{
				var replicationDestination = localReplicationDestinations[i];
				if (ShouldExecuteUsing(replicationDestination, currentRequest, method, false) == false)
					continue;
				if (TryOperation(operation, replicationDestination, true, out result))
					return result;
				if (IsFirstFailure(replicationDestination) && TryOperation(operation, replicationDestination, localReplicationDestinations.Count > i + 1, out result))
					return result;
				IncrementFailureCount(replicationDestination);
			}
			// this should not be thrown, but since I know the value of should...
			throw new InvalidOperationException(@"Attempted to connect to master and all replicas have failed, giving up.
There is a high probability of a network problem preventing access to all the replicas.
Failed to get in touch with any of the " + (1 + localReplicationDestinations.Count) + " Raven instances.");
		}

		protected virtual bool TryOperation<T>(Func<string, T> operation, string operationUrl, bool avoidThrowing, out T result)
		{
			try
			{
				result = operation(operationUrl);
				ResetFailureCount(operationUrl);
				return true;
			}
			catch (Exception e)
			{
				if (avoidThrowing == false)
					throw;
				result = default(T);
				if (IsServerDown(e))
					return false;
				throw;
			}
		}
		#endregion

		#region ExecuteWithReplicationAsync

		public Task<T> ExecuteWithReplicationAsync<T>(string method, string primaryUrl, int currentRequest, int currentReadStripingBase, Func<string, Task<T>> operation)
		{
			return ExecuteWithReplicationAsync(new ExecuteWithReplicationState<T>(method, primaryUrl, currentRequest, currentReadStripingBase, operation));
		}

		private Task<T> ExecuteWithReplicationAsync<T>(ExecuteWithReplicationState<T> state)
		{
			switch (state.State)
			{
				case ExecuteWithReplicationStates.Start:
					state.ReplicationDestinations = ReplicationDestinationsUrls;

					var shouldReadFromAllServers = ((conventions.FailoverBehavior & FailoverBehavior.ReadFromAllServers) ==
													FailoverBehavior.ReadFromAllServers);
					if (shouldReadFromAllServers && state.Method == "GET")
					{
						var replicationIndex = state.ReadStripingBase % (state.ReplicationDestinations.Count + 1);
						// if replicationIndex == destinations count, then we want to use the master
						// if replicationIndex < 0, then we were explicitly instructed to use the master
						if (replicationIndex < state.ReplicationDestinations.Count && replicationIndex >= 0)
						{
							// if it is failing, ignore that, and move to the master or any of the replicas
							if (ShouldExecuteUsing(state.ReplicationDestinations[replicationIndex], state.CurrentRequest, state.Method, false))
							{
								return AttemptOperationAndOnFailureCallExecuteWithReplication(state.ReplicationDestinations[replicationIndex],
																							  state.With(ExecuteWithReplicationStates.AfterTryingWithStripedServer),
																							  state.ReplicationDestinations.Count > state.LastAttempt +1);
							}
						}
					}

					goto case ExecuteWithReplicationStates.AfterTryingWithStripedServer;
				case ExecuteWithReplicationStates.AfterTryingWithStripedServer:

					if (!ShouldExecuteUsing(state.PrimaryUrl, state.CurrentRequest, state.Method, true))
						goto case ExecuteWithReplicationStates.TryAllServers; // skips both checks

					return AttemptOperationAndOnFailureCallExecuteWithReplication(state.PrimaryUrl,
																					state.With(ExecuteWithReplicationStates.AfterTryingWithDefaultUrl),
																					state.ReplicationDestinations.Count > state.LastAttempt + 1);

				case ExecuteWithReplicationStates.AfterTryingWithDefaultUrl:
					if (IsFirstFailure(state.PrimaryUrl))
						return AttemptOperationAndOnFailureCallExecuteWithReplication(state.PrimaryUrl,
																					  state.With(ExecuteWithReplicationStates.AfterTryingWithDefaultUrlTwice),
																					  state.ReplicationDestinations.Count > state.LastAttempt + 1);

					goto case ExecuteWithReplicationStates.AfterTryingWithDefaultUrlTwice;
				case ExecuteWithReplicationStates.AfterTryingWithDefaultUrlTwice:

					IncrementFailureCount(state.PrimaryUrl);

					goto case ExecuteWithReplicationStates.TryAllServers;
				case ExecuteWithReplicationStates.TryAllServers:

					// The following part (cases ExecuteWithReplicationStates.TryAllServers, and ExecuteWithReplicationStates.TryAllServersSecondAttempt)
					// is a for loop, rolled out using goto and nested calls of the method in continuations
					state.LastAttempt++;
					if (state.LastAttempt >= state.ReplicationDestinations.Count)
						goto case ExecuteWithReplicationStates.AfterTryingAllServers;

					var destination = state.ReplicationDestinations[state.LastAttempt];
					if (!ShouldExecuteUsing(destination, state.CurrentRequest, state.Method, false))
					{
						// continue the next iteration of the loop
						goto case ExecuteWithReplicationStates.TryAllServers;
					}

					return AttemptOperationAndOnFailureCallExecuteWithReplication(destination,
																				  state.With(ExecuteWithReplicationStates.TryAllServersSecondAttempt),
																				  state.ReplicationDestinations.Count > state.LastAttempt + 1);
				case ExecuteWithReplicationStates.TryAllServersSecondAttempt:
					destination = state.ReplicationDestinations[state.LastAttempt];
					if (IsFirstFailure(destination))
						return AttemptOperationAndOnFailureCallExecuteWithReplication(destination,
																					  state.With(ExecuteWithReplicationStates.TryAllServersFailedTwice),
																					  state.ReplicationDestinations.Count > state.LastAttempt + 1);

					goto case ExecuteWithReplicationStates.TryAllServersFailedTwice;
				case ExecuteWithReplicationStates.TryAllServersFailedTwice:
					IncrementFailureCount(state.ReplicationDestinations[state.LastAttempt]);

					// continue the next iteration of the loop
					goto case ExecuteWithReplicationStates.TryAllServers;

				case ExecuteWithReplicationStates.AfterTryingAllServers:
					throw new InvalidOperationException(@"Attempted to connect to master and all replicas have failed, giving up.
There is a high probability of a network problem preventing access to all the replicas.
Failed to get in touch with any of the " + (1 + state.ReplicationDestinations.Count) + " Raven instances.");

				default:
					throw new InvalidOperationException("Invalid ExecuteWithReplicationState " + state);
			}
		}

		protected virtual Task<T> AttemptOperationAndOnFailureCallExecuteWithReplication<T>(string url, ExecuteWithReplicationState<T> state, bool avoidThrowing)
		{
			Task<Task<T>> finalTask = state.Operation(url).ContinueWith(task =>
			{
				switch (task.Status)
				{
					case TaskStatus.RanToCompletion:
						ResetFailureCount(url);
						var tcs = new TaskCompletionSource<T>();
						tcs.SetResult(task.Result);
						return tcs.Task;

					case TaskStatus.Canceled:
						tcs = new TaskCompletionSource<T>();
						tcs.SetCanceled();
						return tcs.Task;

					case TaskStatus.Faulted:
						Debug.Assert(task.Exception != null);
						if (IsServerDown(task.Exception) && avoidThrowing)
							return ExecuteWithReplicationAsync(state);

						tcs = new TaskCompletionSource<T>();
						tcs.SetException(task.Exception);
						return tcs.Task;

					default:
						throw new InvalidOperationException("Unknown task status in AttemptOperationAndOnFailureCallExecuteWithReplication");
				}
			});
			return finalTask.Unwrap();
		}

		protected class ExecuteWithReplicationState<T>
		{
			public ExecuteWithReplicationState(string method, string primaryUrl, int currentRequest, int readStripingBase, Func<string, Task<T>> operation)
			{
				Method = method;
				PrimaryUrl = primaryUrl;
				CurrentRequest = currentRequest;
				ReadStripingBase = readStripingBase;
				Operation = operation;

				State = ExecuteWithReplicationStates.Start;
			}

			public readonly string Method;
			public readonly Func<string, Task<T>> Operation;
			public readonly string PrimaryUrl;
			public readonly int CurrentRequest;
			public readonly int ReadStripingBase;

			public ExecuteWithReplicationStates State = ExecuteWithReplicationStates.Start;
			public int LastAttempt = -1;
			public List<string> ReplicationDestinations;

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

		#endregion

		protected virtual bool IsServerDown(Exception e)
		{
			var aggregateException = e as AggregateException;
			if (aggregateException != null)
			{
				e = aggregateException.ExtractSingleInnerException();
			}

			var webException = (e as WebException) ?? (e.InnerException as WebException);
			if (webException != null)
			{
				switch (webException.Status)
				{
#if !SILVERLIGHT
					case WebExceptionStatus.NameResolutionFailure:
					case WebExceptionStatus.ReceiveFailure:
					case WebExceptionStatus.PipelineFailure:
					case WebExceptionStatus.ConnectionClosed:
					case WebExceptionStatus.Timeout:
#endif
					case WebExceptionStatus.ConnectFailure:
					case WebExceptionStatus.SendFailure:
						return true;
				}

				var httpWebResponse = webException.Response as HttpWebResponse;
				if (httpWebResponse != null)
				{
					switch (httpWebResponse.StatusCode)
					{
						case HttpStatusCode.RequestTimeout:
						case HttpStatusCode.BadGateway:
						case HttpStatusCode.ServiceUnavailable:
						case HttpStatusCode.GatewayTimeout:
							return true;
					}
				}
			}
			return e.InnerException is SocketException ||
				e.InnerException is IOException;
		}

		public virtual void Dispose()
		{
			var replicationInformationTaskCopy = refreshReplicationInformationTask;
			if (replicationInformationTaskCopy != null)
				replicationInformationTaskCopy.Wait();
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

	public class ReplicationDestinationData
	{
		public string Url { get; set; }
	}
}
