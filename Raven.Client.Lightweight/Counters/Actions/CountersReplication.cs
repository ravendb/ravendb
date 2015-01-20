using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Json.Linq;

namespace Raven.Client.Counters.Actions
{
	public class ReplicationClient : CountersActionsBase
	{
		internal ReplicationClient(ICounterStore parent, string counterName)
			: base(parent, counterName)
		{
		}

		public async Task<CounterStorageReplicationDocument> GetReplicationsAsync(CancellationToken token = default (CancellationToken))
		{
			var requestUriString = String.Format("{0}/replications/get", counterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<CounterStorageReplicationDocument>(jsonSerializer);
			}
		}

		public async Task SaveReplicationsAsync(CounterStorageReplicationDocument newReplicationDocument,CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/replications/save", counterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
			{
				await request.WriteAsync(RavenJObject.FromObject(newReplicationDocument)).WithCancellation(token).ConfigureAwait(false);
				await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
			}
		}
	}
}