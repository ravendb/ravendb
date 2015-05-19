using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;

namespace Raven.Client.Counters.Actions
{
	public class CountersCommands : CountersActionsBase
	{
		private readonly CountersClient client;

		internal CountersCommands(ICounterStore parent, CountersClient client, string counterStorageName)
			: base(parent, counterStorageName)
		{
			this.client = client;
			
		}

		public async Task ChangeAsync(string groupName, string counterName, long delta, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format(CultureInfo.InvariantCulture,"{0}/change/{1}/{2}?delta={3}",
				CounterStorageUrl, groupName, counterName, delta);

			await Parent.ReplicationInformer.ExecuteWithReplicationAsync(HttpMethods.Post, client, metadata =>
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
			var requestUriString = String.Format("{0}/reset/{1}/{2}", CounterStorageUrl, groupName, counterName);

			await Parent.ReplicationInformer.ExecuteWithReplicationAsync(HttpMethods.Post, client, metadata =>
			{
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
					return request.ReadResponseJsonAsync().WithCancellation(token);
			});
		}

		public async Task<long> GetOverallTotalAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{
			
			return await Parent.ReplicationInformer.ExecuteWithReplicationAsyncWithReturnValue(HttpMethods.Get, client, async metadata =>
			{
				var requestUriString = String.Format("{0}/cs/{1}/getCounterOverallTotal/{2}/{3}", metadata.Url,CounterStorageName, groupName, counterName);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
				{
					var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
					return response.Value<long>();
				}
			});
		}

		public async Task<List<CounterView.ServerValue>> GetServersValuesAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/getCounterServersValues/{1}/{2}", CounterStorageUrl, groupName, counterName);

			return await Parent.ReplicationInformer.ExecuteWithReplicationAsyncWithReturnValue(HttpMethods.Get, client, async metadata =>
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