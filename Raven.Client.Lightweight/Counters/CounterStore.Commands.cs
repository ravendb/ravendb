using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;

namespace Raven.Client.Counters
{
	public partial class CounterStore
    {
		public async Task ChangeAsync(string groupName, string counterName, long delta, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format(CultureInfo.InvariantCulture, "{0}/change/{1}/{2}?delta={3}",
				Url, groupName, counterName, delta);

			await ReplicationInformer.ExecuteWithReplicationAsync(HttpMethods.Post, metadata =>
			{
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
					return request.ReadResponseJsonAsync().WithCancellation(token);
			});
		}

		public async Task IncrementAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{
			await ChangeAsync(groupName, counterName, 1, token);
		}

		public async Task DecrementAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{
			await ChangeAsync(groupName, counterName, -1, token);
		}

		public async Task ResetAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/reset/{1}/{2}", Url, groupName, counterName);

			await ReplicationInformer.ExecuteWithReplicationAsync(HttpMethods.Post,  metadata =>
			{
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
					return request.ReadResponseJsonAsync().WithCancellation(token);
			});
		}

		public async Task<long> GetOverallTotalAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{

			return await ReplicationInformer.ExecuteWithReplicationAsyncWithReturnValue(HttpMethods.Get,  async metadata =>
			{
				var requestUriString = String.Format("{0}/cs/{1}/getCounterOverallTotal/{2}/{3}", metadata.Url, this.DefaultCounterStorageName, groupName, counterName);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
				{
					var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
					return response.Value<long>();
				}
			});
		}

		public async Task<List<CounterView.ServerValue>> GetServersValuesAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/getCounterServersValues/{1}/{2}", Url, groupName, counterName);

			return await ReplicationInformer.ExecuteWithReplicationAsyncWithReturnValue(HttpMethods.Get,  async metadata =>
			{
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
				{
					var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
					return response.ToObject<List<CounterView.ServerValue>>();
				}
			});
		}
    }
}
