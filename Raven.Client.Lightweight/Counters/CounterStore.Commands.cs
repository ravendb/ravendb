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
			AssertInitialized();

			await ReplicationInformer.ExecuteWithReplicationAsync(Url,HttpMethods.Post, url =>
			{
				var requestUriString = String.Format(CultureInfo.InvariantCulture, "{0}/cs/{1}/change/{2}/{3}?delta={4}",
					url, Name, groupName, counterName, delta);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
					return request.ReadResponseJsonAsync().WithCancellation(token);
			},token);
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
			AssertInitialized();


			await ReplicationInformer.ExecuteWithReplicationAsync(Url,HttpMethods.Post,  url =>
			{
				var requestUriString = String.Format("{0}/cs/{1}/reset/{2}/{3}", url, Name, groupName, counterName);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
					return request.ReadResponseJsonAsync().WithCancellation(token);
			},token);
		}

		public async Task<long> GetOverallTotalAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{
			AssertInitialized();

			return await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Get,  async url =>
			{
				var requestUriString = String.Format("{0}/cs/{1}/getCounterOverallTotal/{2}/{3}", url, Name, groupName, counterName);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
				{
					var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
					return response.Value<long>();
				}
			},token);
		}

		public async Task<List<CounterView.ServerValue>> GetServersValuesAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{
			AssertInitialized();


			return await ReplicationInformer.ExecuteWithReplicationAsync(Url,HttpMethods.Get,  async url =>
			{
				var requestUriString = String.Format("{0}/getCounterServersValues/{1}/{2}", url, groupName, counterName);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
				{
					var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
					return response.ToObject<List<CounterView.ServerValue>>();
				}
			},token);
		}
    }
}
