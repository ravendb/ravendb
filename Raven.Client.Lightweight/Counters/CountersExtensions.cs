using System.Net;
using Raven.Abstractions.Counters;
using Raven.Client.Counters.Actions;

namespace Raven.Client.Counters
{
	public static class CountersExtensions
	{
		public static CountersClient NewCountersClient(this IDocumentStore store, string name, ICredentials credentials = null, string apiKey = null)
		{
			return new CountersClient(store.Url,name,credentials, apiKey);
		}

		public static CountersBatchOperation NewBatch(this CountersClient client, CountersBatchOptions options = null)
		{
			return new CountersBatchOperation(client,options);
		}
	}
}
