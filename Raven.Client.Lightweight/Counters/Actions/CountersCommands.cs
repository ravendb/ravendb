using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Counters.Actions
{
	public class CountersCommands : CountersActionsBase
	{
		internal CountersCommands(ICounterStore parent,string counterName) : base(parent, counterName)
		{
		}

		public async Task ChangeAsync(string group, long delta, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format(CultureInfo.InvariantCulture,"{0}/change?group={1}&counterName={2}&delta={3}",
				counterStorageUrl, @group, counterName, delta);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Post))
				await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
		}

		public async Task IncrementAsync(string group, CancellationToken token = default(CancellationToken))
		{
			await ChangeAsync(@group, 1, token);
		}

		public async Task DecrementAsync(string group, CancellationToken token = default(CancellationToken))
		{
			await ChangeAsync(@group, -1, token);
		}

		public async Task ResetAsync(string group, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/change?group={1}&counterName={2}", counterStorageUrl, @group, counterName);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Post))
				await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
		}

		public async Task<long> GetOverallTotalAsync(string group,  CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/getCounterOverallTotal?group={1}&counterName={2}", counterStorageUrl, @group, counterName);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false); ;
				return response.Value<long>();
			}
		}

		public async Task<List<CounterView.ServerValue>> GetServersValuesAsync(string group, CancellationToken token = default(CancellationToken))
		{
			var requestUriString = String.Format("{0}/getCounterServersValues?group={1}&counterName={2}", counterStorageUrl, @group, counterName);

			using (var request = CreateHttpJsonRequest(requestUriString, Verbs.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false); ;
				return response.ToObject<List<CounterView.ServerValue>>();
			}
		}
	}
}