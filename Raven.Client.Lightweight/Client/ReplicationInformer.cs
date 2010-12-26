#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="ReplicationInformer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Bundles.Replication.Data;
using Raven.Database.Json;

namespace Raven.Client.Client
{
	/// <summary>
	/// Replication and failover management on the client side
	/// </summary>
	public class ReplicationInformer
	{
		private const string RavenReplicationDestinations = "Raven/Replication/Destinations";
		private DateTime lastReplicationUpdate = DateTime.MinValue;
		private readonly object replicationLock = new object();
		private List<string> replicationDestinations = new List<string>();

		/// <summary>
		/// Gets the replication destinations.
		/// </summary>
		/// <value>The replication destinations.</value>
		public List<string> ReplicationDestinations
		{
			get { return replicationDestinations; }
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
			RefreshReplicationInformation(serverClient);
		}

		private class IntHolder
		{
			public int Value;
		}

		/// <summary>
		/// Should execute the operation using the specified operation URL
		/// </summary>
		/// <param name="operationUrl">The operation URL.</param>
		/// <param name="currentRequest">The current request.</param>
		/// <returns></returns>
		public bool ShouldExecuteUsing(string operationUrl, int currentRequest)
		{
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
			lock (replicationLock)
			{

				lastReplicationUpdate = DateTime.UtcNow;
				var document = commands.DirectGet(commands.Url, RavenReplicationDestinations);
				failureCounts[commands.Url] = new IntHolder();// we just hit the master, so we can reset its failure count
				if (document == null)
					return;
				var replicationDocument = document.DataAsJson.JsonDeserialization<ReplicationDocument>();
				replicationDestinations = replicationDocument.Destinations.Select(x => x.Url).ToList();
				foreach (var replicationDestination in replicationDestinations)
				{
					IntHolder value;
					if (failureCounts.TryGetValue(replicationDestination, out value))
						continue;
					failureCounts[replicationDestination] = new IntHolder();
				}
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
