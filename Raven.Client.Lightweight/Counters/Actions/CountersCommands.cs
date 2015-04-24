using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;

namespace Raven.Client.Counters.Actions
{
	public class CountersCommands : CountersActionsBase
	{
		private readonly CountersClient client;
		private readonly ICountersReplicationInformer replicationInformer;

		internal CountersCommands(ICounterStore parent, CountersClient client, string counterStorageName, ICountersReplicationInformer replicationInformer)
			: base(parent, counterStorageName)
		{
			if (replicationInformer == null) throw new ArgumentNullException("replicationInformer"); //precaution
			this.client = client;
			this.replicationInformer = replicationInformer;
		}

		public async Task ChangeAsync(string groupName, string counterName, long delta, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format(CultureInfo.InvariantCulture,"{0}/change/{1}/{2}?delta={3}",
				counterStorageUrl, groupName, counterName, delta);

			await replicationInformer.ExecuteWithReplicationAsync(HttpMethods.Post, client, metadata =>
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
			var requestUriString = String.Format("{0}/reset/{1}/{2}", counterStorageUrl, groupName, counterName);

			await replicationInformer.ExecuteWithReplicationAsync(HttpMethods.Post, client, metadata =>
			{
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
					return request.ReadResponseJsonAsync().WithCancellation(token);
			});
		}

		public async Task<long> GetOverallTotalAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/getCounterOverallTotal/{1}/{2}", counterStorageUrl, groupName, counterName);

			return await replicationInformer.ExecuteWithReplicationAsyncWithReturnValue(HttpMethods.Get, client, async metadata =>
			{
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
				{
					var response = await request.ReadResponseJsonAsync().WithCancellation(token);
					return response.Value<long>();
				}
			});
		}

		public async Task<List<CounterView.ServerValue>> GetServersValuesAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/getCounterServersValues/{1}/{2}", counterStorageUrl, groupName, counterName);

			return await replicationInformer.ExecuteWithReplicationAsyncWithReturnValue(HttpMethods.Get, client, async metadata =>
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