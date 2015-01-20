using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Counters.Actions
{
	public class CountersStats : CountersActionsBase
	{
		internal CountersStats(ICounterStore parent, string counterName)
			: base(parent, counterName)
		{
		}

		public async Task<List<CounterStorageStats>> GetCounterStorageStatsAsync(CancellationToken token = default (CancellationToken))
		{
			var requestUriString = String.Format("{0}/stats", counterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString,HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<List<CounterStorageStats>>(jsonSerializer);
			}
		}

		public async Task<List<CountersStorageMetrics>> GetCounterStorageMetricsAsync(CancellationToken token = default (CancellationToken))
		{
			var requestUriString = String.Format("{0}/metrics", counterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<List<CountersStorageMetrics>>(jsonSerializer);
			}
		}

		public async Task<List<CounterStorageReplicationStats>> GetCounterStoragRelicationStatsAsync(CancellationToken token = default (CancellationToken))
		{
			var requestUriString = String.Format("{0}/replications/stats", counterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<List<CounterStorageReplicationStats>>(jsonSerializer);
			}
		}
	}
}