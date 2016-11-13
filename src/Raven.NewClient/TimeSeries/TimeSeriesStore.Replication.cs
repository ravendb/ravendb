using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.TimeSeries
{
    public partial class TimeSeriesStore
    {
        public async Task<TimeSeriesReplicationDocument> GetReplicationsAsync(CancellationToken token = default (CancellationToken))
        {
            AssertInitialized();

            var requestUriString = String.Format("{0}ts/{1}/replications/get", Url, Name);

            using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
            {
                var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return response.ToObject<TimeSeriesReplicationDocument>(JsonSerializer);
            }
        }

        public async Task SaveReplicationsAsync(TimeSeriesReplicationDocument newReplicationDocument,CancellationToken token = default(CancellationToken))
        {
            AssertInitialized();
            var requestUriString = String.Format("{0}ts/{1}/replications/save", Url, Name);

            using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
            {
                await request.WriteAsync(RavenJObject.FromObject(newReplicationDocument)).WithCancellation(token).ConfigureAwait(false);
                await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
            }
        }

        public async Task<long> GetLastEtag(string serverId, CancellationToken token = default(CancellationToken))
        {
            AssertInitialized();
            var requestUriString = String.Format("{0}ts/{1}/lastEtag?serverId={2}", Url, Name, serverId);

            using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
            {
                var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                return response.Value<long>();
            }
        }
    }
}
