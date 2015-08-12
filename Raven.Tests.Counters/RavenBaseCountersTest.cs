using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Client;
using Raven.Client.Counters;
using Raven.Database.Extensions;
using Raven.Tests.Helpers;

namespace Raven.Tests.Counters
{
	public class RavenBaseCountersTest : RavenTestBase
	{
		protected readonly IDocumentStore ravenStore;
		private readonly ConcurrentDictionary<string, int> storeCount;
		protected readonly string DefaultCounterStorageName = "ThisIsRelativelyUniqueCounterName";

		protected RavenBaseCountersTest()
		{
			foreach (var folder in Directory.EnumerateDirectories(Directory.GetCurrentDirectory(), "ThisIsRelativelyUniqueCounterName*"))
				IOExtensions.DeleteDirectory(folder);

			ravenStore = NewRemoteDocumentStore(fiddler:true);
			DefaultCounterStorageName += Guid.NewGuid();
			storeCount = new ConcurrentDictionary<string, int>();
		}

		protected ICounterStore NewRemoteCountersStore(string counterStorageName, bool createDefaultCounter = true,OperationCredentials credentials = null, IDocumentStore ravenStore = null)
		{
			ravenStore = ravenStore ?? this.ravenStore;
			storeCount.AddOrUpdate(ravenStore.Identifier, id => 1, (id, val) => val++);		
	
			var counterStore = new CounterStore
			{
				Url = ravenStore.Url,
				Credentials = credentials ?? new OperationCredentials(null,CredentialCache.DefaultNetworkCredentials),
				Name = counterStorageName + storeCount[ravenStore.Identifier]
			};
			counterStore.Initialize(createDefaultCounter);
			return counterStore;
		}

		public override void Dispose()
		{
			if (ravenStore != null) ravenStore.Dispose();

			try
			{
				base.Dispose();
			}
			catch (AggregateException) //TODO: do not forget to investigate where counter storage is not being disposed
			{
			}
		}

		protected async Task<bool> WaitForReplicationBetween(ICounterStore source, ICounterStore destination, string groupName, string counterName, int timeoutInSec = 30)
		{
			var waitStartingTime = DateTime.Now;
			var hasReplicated = false;

			if (Debugger.IsAttached)
				timeoutInSec = 60 * 60; //1 hour timeout if debugging

			while (true)
			{
				if ((DateTime.Now - waitStartingTime).TotalSeconds > timeoutInSec)
					break;

				var sourceValue = await source.GetOverallTotalAsync(groupName, counterName);
				var targetValue = await destination.GetOverallTotalAsync(groupName, counterName);
				if (sourceValue == targetValue)
				{
					hasReplicated = true;
					break;
				}

				Thread.Sleep(50);
			}

			return hasReplicated;
		}

		protected static async Task<object> SetupReplicationAsync(ICounterStore source, params ICounterStore[] destinations)
		{
			var replicationDocument = new CountersReplicationDocument();
			foreach (var destStore in destinations)
			{
				replicationDocument.Destinations.Add(new CounterReplicationDestination
				{
					CounterStorageName = destStore.Name,
					ServerUrl = destStore.Url
				});
			}

			await source.SaveReplicationsAsync(replicationDocument);
			return null;
		}
	}
}
