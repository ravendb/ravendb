#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="ReplicationInformer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.IsolatedStorage;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
#if !NET_3_5
using System.Threading.Tasks;
#endif
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Client.Document;

namespace Raven.Client.Connection
{
	/// <summary>
	/// Replication and failover management on the client side
	/// </summary>
	public class ReplicationInformer : IDisposable
	{
		private readonly ILog log = LogProvider.GetCurrentClassLogger();

		private readonly DocumentConvention conventions;
		private const string RavenReplicationDestinations = "Raven/Replication/Destinations";
		private DateTime lastReplicationUpdate = DateTime.MinValue;
		private readonly object replicationLock = new object();
		private List<string> replicationDestinations = new List<string>();
		private static readonly List<string> Empty = new List<string>();
		private int readStripingBase;

		/// <summary>
		/// Notify when the failover status changed
		/// </summary>
		public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged = delegate { };


		/// <summary>
		/// Gets the replication destinations.
		/// </summary>
		/// <value>The replication destinations.</value>
		public List<string> ReplicationDestinations
		{
			get
			{
				if (conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
					return Empty;

				return replicationDestinations;
			}
		}

		///<summary>
		/// Create a new instance of this class
		///</summary>
		public ReplicationInformer(DocumentConvention conventions)
		{
			this.conventions = conventions;
		}

#if !NET_3_5
		private readonly System.Collections.Concurrent.ConcurrentDictionary<string, IntHolder> failureCounts = new System.Collections.Concurrent.ConcurrentDictionary<string, IntHolder>();
		private Task refreshReplicationInformationTask;
#else
		private readonly Dictionary<string, IntHolder> failureCounts = new Dictionary<string, IntHolder>();
#endif

#if NET_3_5
		/// <summary>
		/// Updates the replication information if needed.
		/// </summary>
		/// <param name="serverClient">The server client.</param>
		public void UpdateReplicationInformationIfNeeded(ServerClient serverClient)
		{
			if (conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
				return;

			if (lastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
				return;
			lock (replicationLock)
			{
				if (lastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
					return;

				try 
				{          
					RefreshReplicationInformation(serverClient);
				}
				catch (System.Exception e)
				{
					log.ErrorException("Failed to refresh replication information", e);
				}
			}
		}
#else
		/// <summary>
		/// Updates the replication information if needed.
		/// </summary>
		/// <param name="serverClient">The server client.</param>
		public Task UpdateReplicationInformationIfNeeded(ServerClient serverClient)
		{
			if (conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
				return new CompletedTask();

			if (lastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
				return new CompletedTask();

			lock (replicationLock)
			{
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
#endif

		private class IntHolder
		{
			public int Value;
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
		public bool ShouldExecuteUsing(string operationUrl, int currentRequest, string method, bool primary)
		{
			if (primary == false)
				AssertValidOperation(method);

			IntHolder value = GetHolder(operationUrl);
			if (value.Value > 1000)
			{
				return currentRequest % 1000 == 0;
			}
			if (value.Value > 100)
			{
				return currentRequest % 100 == 0;
			}
			if (value.Value > 10)
			{
				return currentRequest % 10 == 0;
			}
			return true;
		}

		private void AssertValidOperation(string method)
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

		private IntHolder GetHolder(string operationUrl)
		{
#if !NET_3_5
			return failureCounts.GetOrAdd(operationUrl, new IntHolder());
#else
	// need to compensate for 3.5 not having concnurrent dic.

			IntHolder value;
			if(failureCounts.TryGetValue(operationUrl, out value) == false)
			{
				lock(replicationLock)
				{
					if(failureCounts.TryGetValue(operationUrl, out value) == false)
					{
						failureCounts[operationUrl] = value = new IntHolder();
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
			IntHolder value = GetHolder(operationUrl);
			return Thread.VolatileRead(ref value.Value) == 0;
		}

		/// <summary>
		/// Increments the failure count for the specified operation URL
		/// </summary>
		/// <param name="operationUrl">The operation URL.</param>
		public void IncrementFailureCount(string operationUrl)
		{
			IntHolder value = GetHolder(operationUrl);
			var current = Interlocked.Increment(ref value.Value);
			if(current == 1)// first failure
			{
				FailoverStatusChanged(this, new FailoverStatusChangedEventArgs
				{
					Url = operationUrl,
					Failing = true
				});
			}
		}

		/// <summary>
		/// Refreshes the replication information.
		/// Expert use only.
		/// </summary>
		[MethodImpl(MethodImplOptions.Synchronized)]
		public void RefreshReplicationInformation(ServerClient commands)
		{
			var serverHash = GetServerHash(commands);

			JsonDocument document;
			try
			{
				document = commands.DirectGet(commands.Url, RavenReplicationDestinations);
				failureCounts[commands.Url] = new IntHolder(); // we just hit the master, so we can reset its failure count
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

			var replicationDocument = document.DataAsJson.JsonDeserialization<ReplicationDocument>();
			replicationDestinations = replicationDocument.Destinations.Select(x => x.Url)
				// filter out replication destination that don't have the url setup, we don't know how to reach them
				// so we might as well ignore them. Probably private replication destination (using connection string names only)
				.Where(x => x != null)
				.ToList();
			foreach (var replicationDestination in replicationDestinations)
			{
				IntHolder value;
				if (failureCounts.TryGetValue(replicationDestination, out value))
					continue;
				failureCounts[replicationDestination] = new IntHolder();
			}

			lastReplicationUpdate = SystemTime.UtcNow;
		}

		private JsonDocument TryLoadReplicationInformationFromLocalCache(string serverHash)
		{
			try
			{
				using (var machineStoreForApplication = IsolatedStorageFile.GetMachineStoreForDomain())
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
				using (var machineStoreForApplication = IsolatedStorageFile.GetMachineStoreForDomain())
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

		private static string GetServerHash(ServerClient commands)
		{
			using (var md5 = MD5.Create())
			{
				return BitConverter.ToString(md5.ComputeHash(Encoding.UTF8.GetBytes(commands.Url)));
			}
		}


		/// <summary>
		/// Resets the failure count for the specified URL
		/// </summary>
		/// <param name="operationUrl">The operation URL.</param>
		public void ResetFailureCount(string operationUrl)
		{
			IntHolder value = GetHolder(operationUrl);
			var oldVal = value.Value;
			Thread.VolatileWrite(ref value.Value, 0);
			if(oldVal != 0)
			{
				FailoverStatusChanged(this,
					new FailoverStatusChangedEventArgs
					{
						Url = operationUrl,
						Failing = false
					});
			}
		}

		public int GetReadStripingBase()
		{
			return Interlocked.Increment(ref readStripingBase);
		}

		public void Dispose()
		{
#if !NET_3_5
			var replicationInformationTaskCopy = refreshReplicationInformationTask;
			if (replicationInformationTaskCopy != null)
				replicationInformationTaskCopy.Wait();
#endif
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
#endif