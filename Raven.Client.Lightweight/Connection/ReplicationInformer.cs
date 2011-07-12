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
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Document;

namespace Raven.Client.Connection
{
	/// <summary>
	/// Replication and failover management on the client side
	/// </summary>
	public class ReplicationInformer
	{
		private Logger log = LogManager.GetCurrentClassLogger();

		private readonly DocumentConvention conventions;
		private const string RavenReplicationDestinations = "Raven/Replication/Destinations";
		private DateTime lastReplicationUpdate = DateTime.MinValue;
		private readonly object replicationLock = new object();
		private List<string> replicationDestinations = new List<string>();
		private static readonly List<string> Empty = new List<string>();

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
#else
		private readonly Dictionary<string, IntHolder> failureCounts = new Dictionary<string, IntHolder>();
#endif

		/// <summary>
		/// Updates the replication information if needed.
		/// </summary>
		/// <param name="serverClient">The server client.</param>
		public void UpdateReplicationInformationIfNeeded(ServerClient serverClient)
		{
			if (lastReplicationUpdate.AddMinutes(5) > DateTime.UtcNow)
				return;
			lock (replicationLock)
			{
				if (lastReplicationUpdate.AddMinutes(5) > DateTime.UtcNow)
					return;
				RefreshReplicationInformation(serverClient);
			}
		}

		private class IntHolder
		{
			public int Value;
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
			switch (conventions.FailoverBehavior)
			{
				case FailoverBehavior.AllowReadsFromSecondaries:
					if (method == "GET")
						return;
					break;
				case FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries:
					return;
				case FailoverBehavior.FailImmediately:
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
			Interlocked.Increment(ref value.Value);
		}

		/// <summary>
		/// Refreshes the replication information.
		/// </summary>
		/// <param name="commands">The commands.</param>
		public void RefreshReplicationInformation(ServerClient commands)
		{
			var serverHash = GetServerHash(commands);

			lock (replicationLock)
			{

				lastReplicationUpdate = DateTime.UtcNow;
				JsonDocument document;
				try
				{
					document = commands.DirectGet(commands.Url, RavenReplicationDestinations);
					failureCounts[commands.Url] = new IntHolder();// we just hit the master, so we can reset its failure count
					TrySavingReplicationInformationToLocalCache(serverHash, document);
				}
				catch (Exception e)
				{
					log.ErrorException("Could not contact master for new replication information", e);
					document = TryLoadReplicationInformationFromLocalCache(serverHash);
				}
				if (document == null)
					return;



				var replicationDocument = document.DataAsJson.JsonDeserialization<ReplicationDocument>();
				replicationDestinations = replicationDocument.Destinations.Select(x => x.Url)
					// filter out replication destination that don't have the url setup, we don't know how to reach them
					// so we might as well ignore them. Probably private replication destination (using connection string names only)
					.Where(x=>x!=null) 
					.ToList();
				foreach (var replicationDestination in replicationDestinations)
				{
					IntHolder value;
					if (failureCounts.TryGetValue(replicationDestination, out value))
						continue;
					failureCounts[replicationDestination] = new IntHolder();
				}
			}
		}

		private JsonDocument TryLoadReplicationInformationFromLocalCache(string serverHash)
		{
			using (var machineStoreForApplication = IsolatedStorageFile.GetMachineStoreForApplication())
			{
				var path = "RavenDB Replication Information For - " + serverHash;
				if (machineStoreForApplication.FileExists(path) == false)
					return null;
				using (
					var stream = machineStoreForApplication.OpenFile(path,
					                                                 FileMode.Open))
				{
					return stream.ToJObject().ToJsonDocument();
				}
			}
		}

		private void TrySavingReplicationInformationToLocalCache(string serverHash, JsonDocument document)
		{
			using(var machineStoreForApplication = IsolatedStorageFile.GetMachineStoreForApplication())
			using (var stream = machineStoreForApplication.CreateFile("RavenDB Replication Information For - " + serverHash))
			{
				document.ToJson().WriteTo(stream);
			}
		}

		private static string GetServerHash(ServerClient commands)
		{
			using (var md5 = MD5.Create())
			{
				return Convert.ToBase64String(md5.ComputeHash(Encoding.UTF8.GetBytes(commands.Url)));
			}
		}


		/// <summary>
		/// Resets the failure count for the specified URL
		/// </summary>
		/// <param name="operationUrl">The operation URL.</param>
		public void ResetFailureCount(string operationUrl)
		{
			IntHolder value = GetHolder(operationUrl);
			Thread.VolatileWrite(ref value.Value, 0);
		}
	}
}
#endif
