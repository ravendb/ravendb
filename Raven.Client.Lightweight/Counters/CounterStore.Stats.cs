using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;

namespace Raven.Client.Counters
{
	public partial class CounterStore
	{
		public async Task<CounterStorageStats> GetCounterStatsAsync(CancellationToken token = default (CancellationToken))
		{
			AssertInitialized();
			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false); 
			var requestUriString = String.Format("{0}/cs/{1}/stats", Url, Name);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<CounterStorageStats>(JsonSerializer);
			}
		}

		public async Task<CountersStorageMetrics> GetCounterMetricsAsync(CancellationToken token = default (CancellationToken))
		{
			AssertInitialized();
			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false); 
			var requestUriString = String.Format("{0}/cs/{1}/metrics", Url, Name);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<CountersStorageMetrics>(JsonSerializer);
			}
		}

		public async Task<List<CounterStorageReplicationStats>> GetCounterReplicationStatsAsync(CancellationToken token = default (CancellationToken))
		{
			AssertInitialized();
			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false); 
			var requestUriString = String.Format("{0}/cs/{1}/replications/stats", Url, Name);

			using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
			{
				var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
				return response.ToObject<List<CounterStorageReplicationStats>>(JsonSerializer);
			}
		}
	}
}