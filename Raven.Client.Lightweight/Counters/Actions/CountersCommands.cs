using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Counters.Actions
{
	public class CountersCommands : CountersActionsBase
	{
		internal CountersCommands(ICounterStore parent, string counterStorageName)
			: base(parent, counterStorageName)
		{
		}

		public async Task ChangeAsync(string groupName, string counterName, long delta, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format(CultureInfo.InvariantCulture,"{0}/change/{1}/{2}?delta={3}",
				counterStorageUrl, groupName, counterName, delta);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
				await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
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
			var requestUriString = String.Format("{0}/change/{1}/{2}", counterStorageUrl, groupName, counterName);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
				await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
		}

		public async Task<long> GetOverallTotalAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/getCounterOverallTotal/{1}/{2}", counterStorageUrl, groupName, counterName);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false); ;
				return response.Value<long>();
			}
		}

		public async Task<List<CounterView.ServerValue>> GetServersValuesAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/getCounterServersValues/{1}/{2}", counterStorageUrl, groupName, counterName);
			
			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<List<CounterView.ServerValue>>();
			}
		}
	}
}