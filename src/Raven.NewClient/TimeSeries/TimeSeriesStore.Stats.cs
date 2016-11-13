using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;

namespace Raven.NewClient.Client.TimeSeries
{
    public partial class TimeSeriesStore
    {
        public async Task<TimeSeriesStats> GetStatsAsync(CancellationToken token = default (CancellationToken))
        {
            AssertInitialized();
            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false); 
            var requestUriString = String.Format("{0}ts/{1}/stats", Url, Name);

            using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
            {
                var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return response.ToObject<TimeSeriesStats>(JsonSerializer);
            }
        }

        public async Task<TimeSeriesMetrics> GetTimeSeriesMetricsAsync(CancellationToken token = default (CancellationToken))
        {
            AssertInitialized();
            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false); 
            var requestUriString = String.Format("{0}ts/{1}/metrics", Url, Name);

            using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
            {
                var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return response.ToObject<TimeSeriesMetrics>(JsonSerializer);
            }
        }

        public async Task<List<TimeSeriesReplicationStats>> GetTimeSeriesReplicationStatsAsync(CancellationToken token = default (CancellationToken))
        {
            AssertInitialized();
            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false); 
            var requestUriString = String.Format("{0}ts/{1}/replications/stats", Url, Name);

            using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
            {
                var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return response.ToObject<List<TimeSeriesReplicationStats>>(JsonSerializer);
            }
        }
    }
}
