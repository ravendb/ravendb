using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Counters;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;

namespace Raven.NewClient.Client.Counters
{
    public partial class CounterStore
    {
        public async Task<CounterStorageStats> GetCounterStatsAsync(CancellationToken token = default (CancellationToken))
        {
            AssertInitialized();
            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false); 
            var requestUriString = $"{Url}/cs/{Name}/stats";

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
            var requestUriString = $"{Url}/cs/{Name}/metrics";

            using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
            {
                var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return response.ToObject<CountersStorageMetrics>(JsonSerializer);
            }
        }

        public async Task<IReadOnlyList<CounterStorageReplicationStats>> GetCounterReplicationStatsAsync(
            CancellationToken token = default (CancellationToken),
            int skip = 0, int take = 1024)
        {
            AssertInitialized();
            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false); 
            var requestUriString = $"{Url}/cs/{Name}/replications/stats&skip={skip}&take={take}";

            using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
            {
                var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return response.ToObject<List<CounterStorageReplicationStats>>(JsonSerializer);
            }
        }
    }
}
