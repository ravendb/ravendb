using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Bundles.Replication.Data;
using Raven.Database.Json;

namespace Raven.Client.Client
{
	public class ReplicationInformer
	{
		private const string RavenReplicationDestinations = "Raven/Replication/Destinations";
		private DateTime lastReplicationUpdate = DateTime.MinValue;
		private readonly object replicationLock = new object();
		private List<string> replicationDestinations = new List<string>();

		public List<string> ReplicationDestinations
		{
			get { return replicationDestinations; }
		}

		private readonly Dictionary<string, IntHolder> failureCounts = new Dictionary<string, IntHolder>();

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

		public bool ShouldExecuteUsing(string operationUrl, int currentRequest)
		{
			IntHolder value;
			if (failureCounts.TryGetValue(operationUrl, out value) == false)
				throw new KeyNotFoundException("BUG: Could not find failure count for " + operationUrl);
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

		public bool IsFirstFailure(string operationUrl)
		{
			IntHolder value;
			if (failureCounts.TryGetValue(operationUrl, out value) == false)
				throw new KeyNotFoundException("BUG: Could not find failure count for " + operationUrl);
			return Thread.VolatileRead(ref value.Value) == 0;
		}

		public void IncrementFailureCount(string operationUrl)
		{
			IntHolder value;
			if (failureCounts.TryGetValue(operationUrl, out value) == false)
				throw new KeyNotFoundException("BUG: Could not find failure count for " + operationUrl);
			Interlocked.Increment(ref value.Value);
		}

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


		public void ResetFailureCount(string operationUrl)
		{
			IntHolder value;
			if (failureCounts.TryGetValue(operationUrl, out value) == false)
				throw new KeyNotFoundException("BUG: Could not find failure count for " + operationUrl);
			Thread.VolatileWrite(ref value.Value, 0);

		}
	}
}