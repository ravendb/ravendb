using System;
using System.Globalization;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;

namespace Raven.Client.Counters
{
    public partial class CounterStore
    {
        public async Task ChangeAsync(string groupName, string counterName, long delta, CancellationToken token = default(CancellationToken))
        {
            AssertInitialized();
            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().WithCancellation(token).ConfigureAwait(false);
            await ReplicationInformer.ExecuteWithReplicationAsync(Url,HttpMethods.Post, async (url, counterStoreName) =>
            {
                var requestUriString = $"{url}/cs/{counterStoreName}/change?groupName={groupName}&counterName={counterName}&delta={delta}";
                using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
            }, token).WithCancellation(token).ConfigureAwait(false);
        }

        public async Task IncrementAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
        {
            await ChangeAsync(groupName, counterName, 1, token).ConfigureAwait(false);
        }

        public async Task DecrementAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
        {
            await ChangeAsync(groupName, counterName, -1, token).ConfigureAwait(false);
        }

        public async Task ResetAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
        {
            AssertInitialized();
            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().WithCancellation(token).ConfigureAwait(false);

            await ReplicationInformer.ExecuteWithReplicationAsync(Url,HttpMethods.Post,  async (url, counterStoreName) =>
            {
                var requestUriString = $"{url}/cs/{counterStoreName}/reset?groupName={groupName}&counterName={counterName}";
                using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
            }, token).WithCancellation(token).ConfigureAwait(false);
        }

        public async Task DeleteAsync(string groupName, string counterName, CancellationToken token = new CancellationToken())
        {
            AssertInitialized();
            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().WithCancellation(token).ConfigureAwait(false);

            await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Post, async (url, counterStoreName) =>
            {
                var requestUriString = $"{url}/cs/{counterStoreName}/delete?groupName={groupName}&counterName={counterName}";
                using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
            }, token).WithCancellation(token).ConfigureAwait(false);
        }

        public async Task<long> GetOverallTotalAsync(string groupName, string counterName, CancellationToken token = default(CancellationToken))
        {
            AssertInitialized();

            return await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Get, async (url, counterStoreName) =>
            {
                var requestUriString = $"{url}/cs/{counterStoreName}/getCounterOverallTotal?groupName={groupName}&counterName={counterName}";
                using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
                {
                    try
                    {
                        var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                        return response.Value<long>();
                    }
                    catch (ErrorResponseException e)
                    {
                        if (e.StatusCode == HttpStatusCode.NotFound)
                            throw new InvalidOperationException(e.Message, e);
                        throw;
                    }
                }
            }, token).WithCancellation(token).ConfigureAwait(false);
        }

    }
}
