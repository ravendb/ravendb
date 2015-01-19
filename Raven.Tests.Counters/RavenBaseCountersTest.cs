using System.Net;
using Raven.Abstractions.Connection;
using Raven.Client;
using Raven.Client.Counters;
using Raven.Tests.Helpers;

namespace Raven.Tests.Counters
{
	public class RavenBaseCountersTest : RavenTestBase
	{
		protected IDocumentStore ravenStore;
		protected const string DefaultCounteName = "FooBarCounter_ThisIsRelativelyUniqueCounterName";

		public RavenBaseCountersTest()
		{
			ravenStore = NewRemoteDocumentStore(fiddler:true);
		}

		public ICounterStore NewRemoteCountersStore(string counterName = DefaultCounteName,bool createDefaultCounter = false,OperationCredentials credentials = null)
		{
			var counterStore = new CounterStore
			{
				Url = ravenStore.Url,
				Credentials = credentials ?? new OperationCredentials(null,CredentialCache.DefaultNetworkCredentials),
				DefaultCounterName = counterName
			};

			counterStore.Initialize(createDefaultCounter);
			return counterStore;
		}

		public override void Dispose()
		{
			base.Dispose();

			if (ravenStore != null) ravenStore.Dispose();
		}
	}
}
