using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Counters.Actions;
using Raven.Json.Linq;

namespace Raven.Client.Counters.Replication
{
	public class ReplicationClient : CountersActionsBase
	{
		internal ReplicationClient(ICounterStore parent, string counterStorageName)
			: base(parent, counterStorageName)
		{
		}

		public async Task<CountersReplicationDocument> GetReplicationsAsync(CancellationToken token = default (CancellationToken))
		{
			var requestUriString = String.Format("{0}/replications/get", CounterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<CountersReplicationDocument>(JsonSerializer);
			}
		}

		public async Task SaveReplicationsAsync(CountersReplicationDocument newReplicationDocument,CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/replications/save", CounterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
			{
				await request.WriteAsync(RavenJObject.FromObject(newReplicationDocument)).WithCancellation(token).ConfigureAwait(false);
				await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
			}
		}

		public async Task<long> GetLastEtag(string serverId, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/lastEtag?serverId={1}", CounterStorageUrl, serverId);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.Value<long>();
			}
		}
	}
}