using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Util;
using Raven.NewClient.Json.Linq;

namespace Raven.NewClient.Client.TimeSeries
{
    public partial class TimeSeriesStore
    {
        public async Task CreateTypeAsync(string type, string[] fields, CancellationToken token = new CancellationToken())
        {
            AssertInitialized();

            if (string.IsNullOrEmpty(type))
                throw new InvalidOperationException("Prefix cannot be empty");

            if (fields.Length < 1)
                throw new InvalidOperationException("Number of fields should be at least 1");

            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);
            await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Put, async (url, timeSeriesName) =>
            {
                var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/types/{2}",
                    url, timeSeriesName, type);
                using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Put))
                {
                    await request.WriteWithObjectAsync(new TimeSeriesType {Type = type, Fields = fields}).ConfigureAwait(false);
                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token).ConfigureAwait(false);
        }

        public async Task DeleteTypeAsync(string type, CancellationToken token = default(CancellationToken))
        {
            AssertInitialized();
            
            if (string.IsNullOrEmpty(type))
                throw new InvalidOperationException("Prefix cannot be empty");

            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);
            await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Delete, async (url, timeSeriesName) =>
            {
                var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/types/{2}",
                    url, timeSeriesName, type);
                using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
                {
                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token).ConfigureAwait(false);
        }

        public Task AppendAsync(string type, string key, DateTimeOffset at, double value, CancellationToken token = new CancellationToken())
        {
            return AppendAsync(type, key, at, token, value);
        }

        [Obsolete("You must use DateTimeOffset", true)]
        public Task AppendAsync(string type, string key, DateTime at, double value, CancellationToken token = new CancellationToken())
        {
            throw new InvalidOperationException("Must use DateTimeOffset");
        }

        [Obsolete("You must use DateTimeOffset", true)]
        public Task AppendAsync(string type, string key, DateTime at, CancellationToken token, params double[] values)
        {
            throw new InvalidOperationException("Must use DateTimeOffset");
        }

        public async Task AppendAsync(string type, string key, DateTimeOffset at, CancellationToken token, params double[] values)
        {
            AssertInitialized();

            if (string.IsNullOrEmpty(type) || string.IsNullOrEmpty(key) || at < DateTimeOffset.MinValue || values == null || values.Length == 0)
                throw new InvalidOperationException("Append data is invalid");

            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);
            await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Put, async (url, timeSeriesName) =>
            {
                var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/append/{2}?key={3}",
                    url, timeSeriesName, type, Uri.EscapeDataString(key));
                using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Put))
                {
                    await request.WriteWithObjectAsync(new TimeSeriesFullPoint
                    {
                        Type = type,
                        Key = key,
                        At = at,
                        Values = values
                    }).ConfigureAwait(false);
                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token).ConfigureAwait(false);
        }

        [Obsolete("You must use DateTimeOffset", true)]
        public Task AppendAsync(string type, string key, DateTime at, double[] values, CancellationToken token = new CancellationToken())
        {
            throw new InvalidOperationException("Must use DateTimeOffset");
        }

        public Task AppendAsync(string type, string key, DateTimeOffset at, double[] values, CancellationToken token = new CancellationToken())
        {
            return AppendAsync(type, key, at, token, values);
        }

        public async Task DeleteKeyAsync(string type, string key, CancellationToken token = new CancellationToken())
        {
            AssertInitialized();

            if (string.IsNullOrEmpty(type) || string.IsNullOrEmpty(key))
                throw new InvalidOperationException("Data is invalid");

            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);
            await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Post,async (url, timeSeriesName) =>
            {
                var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/delete-key/{2}?key={3}",
                    url, timeSeriesName, type, Uri.EscapeDataString(key));
                using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
                {
                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token).ConfigureAwait(false);
        }

        public Task DeletePointAsync(string type, string key, DateTimeOffset at, CancellationToken token = new CancellationToken())
        {
            var point = new TimeSeriesPointId {Type = type, Key = key, At = at};
            return DeletePointsAsync(new[] {point}, token);
        }

        public async Task DeletePointsAsync(IEnumerable<TimeSeriesPointId> points, CancellationToken token = new CancellationToken())
        {
            AssertInitialized();

            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);
            await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Post, async (url, timeSeriesName) =>
            {
                var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/delete-points",
                    url, timeSeriesName);
                using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
                {
                    await request.WriteWithObjectAsync(points).ConfigureAwait(false);
                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token).ConfigureAwait(false);
        }

        public async Task DeleteRangeAsync(string type, string key, DateTimeOffset start, DateTimeOffset end, CancellationToken token = new CancellationToken())
        {
            AssertInitialized();

            if (string.IsNullOrEmpty(type) || string.IsNullOrEmpty(key))
                throw new InvalidOperationException("Data is invalid");

            if (start > end)
                throw new InvalidOperationException("start cannot be greater than end");

            await ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);
            await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Post, async (url, timeSeriesName) =>
            {
                var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/delete-range/{2}?key={3}",
                    url, timeSeriesName, type, Uri.EscapeDataString(key));
                using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
                {
                    await request.WriteWithObjectAsync(new TimeSeriesDeleteRange
                    {
                        Type = type, Key = key, Start = start, End = end
                    }).ConfigureAwait(false);
                    return await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
                }
            }, token).ConfigureAwait(false);
        }
    }
}
