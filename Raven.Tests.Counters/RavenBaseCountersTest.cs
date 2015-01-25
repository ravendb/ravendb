using System.Collections.Generic;
using System.Net;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Client;
using Raven.Client.Counters;
using Raven.Tests.Helpers;

namespace Raven.Tests.Counters
{
	public class RavenBaseCountersTest : RavenTestBase
	{
		protected IDocumentStore ravenStore;
		protected const string DefaultCounteStorageName = "FooBarCounter_ThisIsRelativelyUniqueCounterName";

		public RavenBaseCountersTest()
		{
			ravenStore = NewRemoteDocumentStore(fiddler:true);
		}

		public ICounterStore NewRemoteCountersStore(string counterStorageName = DefaultCounteStorageName,bool createDefaultCounter = false,OperationCredentials credentials = null)
		{
			var counterStore = new CounterStore
			{
				Url = ravenStore.Url,
				Credentials = credentials ?? new OperationCredentials(null,CredentialCache.DefaultNetworkCredentials),
				DefaultCounterStorageName = counterStorageName
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
	}
}
