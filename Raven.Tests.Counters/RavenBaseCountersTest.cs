using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Client;
using Raven.Client.Counters;
using Raven.Tests.Helpers;

namespace Raven.Tests.Counters
{
	public class RavenBaseCountersTest : RavenTestBase
	{
		private readonly IDocumentStore ravenStore;
		private readonly ConcurrentDictionary<string, int> storeCount;
		protected const string DefaultCounteStorageName = "FooBarCounter_ThisIsRelativelyUniqueCounterName";

		protected RavenBaseCountersTest()
		{
			ravenStore = NewRemoteDocumentStore(fiddler:true);
			storeCount = new ConcurrentDictionary<string, int>();
		}

		protected ICounterStore NewRemoteCountersStore(string counterStorageName = DefaultCounteStorageName, bool createDefaultCounter = true,OperationCredentials credentials = null, IDocumentStore ravenStore = null)
		{
			ravenStore = ravenStore ?? this.ravenStore;
			storeCount.AddOrUpdate(ravenStore.Identifier, id => 1, (id, val) => val++);		
	
			var counterStore = new CounterStore
			{
				Url = ravenStore.Url,
				Credentials = credentials ?? new OperationCredentials(null,CredentialCache.DefaultNetworkCredentials),
				DefaultCounterStorageName = counterStorageName + storeCount[ravenStore.Identifier]
			};
			counterStore.Initialize(createDefaultCounter);
			return counterStore;
		}

		protected CounterStorageDocument CreateCounterStorageDocument(string counterName)
		{
			return new CounterStorageDocument
			{
				Settings = new Dictionary<string, string>
				{
					{ "Raven/Counters/DataDir", @"~\Counters\" + counterName }
				},
			};
		}

		public override void Dispose()
		{
			base.Dispose();

			if (ravenStore != null) ravenStore.Dispose();
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

				using (var sourceClient = source.NewCounterClient())
				using (var destinationClient = destination.NewCounterClient())
				{
					var sourceValue = await sourceClient.Commands.GetOverallTotalAsync(groupName, counterName);
					var targetValue = await destinationClient.Commands.GetOverallTotalAsync(groupName, counterName);
					if (sourceValue == targetValue)
					{
						hasReplicated = true;
						break;
					}
				}

				Thread.Sleep(50);
			}

			return hasReplicated;
		}

		protected static async Task SetupReplicationAsync(ICounterStore source, params ICounterStore[] destinations)
		{
			using (var client = source.NewCounterClient())
			{
				var replicationDocument = new CountersReplicationDocument();
				foreach (var destStore in destinations)
				{
					using (var destClient = destStore.NewCounterClient())
					{
						replicationDocument.Destinations.Add(new CounterReplicationDestination
						{
							CounterStorageName = destClient.CounterStorageName,
							ServerUrl = destClient.ServerUrl
						});

					}
				}

				await client.Replication.SaveReplicationsAsync(replicationDocument);
			}
		}	

	}
}
