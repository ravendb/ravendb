using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;

namespace Raven.Client.Counters.Actions
{
	public class CountersStats : CountersActionsBase
	{
		internal CountersStats(ICounterStore parent,string counterStorageName)
			: base(parent, counterStorageName)
		{
		}

		public async Task<CounterStorageStats> GetCounterStatsAsync(CancellationToken token = default (CancellationToken))
		{
			var requestUriString = String.Format("{0}/stats", CounterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<CounterStorageStats>(JsonSerializer);
			}
		}

		public async Task<CountersStorageMetrics> GetCounterMetricsAsync(CancellationToken token = default (CancellationToken))
		{
			var requestUriString = String.Format("{0}/metrics", CounterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<CountersStorageMetrics>(JsonSerializer);
			}
		}

		public async Task<List<CounterStorageReplicationStats>> GetCounterRelicationStatsAsync(CancellationToken token = default (CancellationToken))
		{
			var requestUriString = String.Format("{0}/replications/stats", CounterStorageUrl);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<List<CounterStorageReplicationStats>>(JsonSerializer);
			}
		}
	}
}