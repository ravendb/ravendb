using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;

namespace Raven.Client.TimeSeries
{
	public partial class TimeSeriesStore
    {
		public async Task ChangeAsync(string groupName, string timeSeriesName, long delta, CancellationToken token = default(CancellationToken))
		{
			AssertInitialized();
			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			await ReplicationInformer.ExecuteWithReplicationAsync(Url,HttpMethods.Post, (url, timeSeriesStoreName) =>
			{
				var requestUriString = String.Format(CultureInfo.InvariantCulture, "{0}/ts/{1}/change/{2}/{3}?delta={4}",
					url, timeSeriesStoreName, groupName, timeSeriesName, delta);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
					return request.ReadResponseJsonAsync().WithCancellation(token);
			},token);
		}

		public async Task IncrementAsync(string groupName, string timeSeriesName, CancellationToken token = default(CancellationToken))
		{
			await ChangeAsync(groupName, timeSeriesName, 1, token);
		}

		public async Task DecrementAsync(string groupName, string timeSeriesName, CancellationToken token = default(CancellationToken))
		{
			await ChangeAsync(groupName, timeSeriesName, -1, token);
		}

		public async Task ResetAsync(string groupName, string timeSeriesName, CancellationToken token = default(CancellationToken))
		{
			AssertInitialized();
			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();

			await ReplicationInformer.ExecuteWithReplicationAsync(Url,HttpMethods.Post,  (url, timeSeriesStoreName) =>
			{
				var requestUriString = String.Format("{0}/ts/{1}/reset/{2}/{3}", url, timeSeriesStoreName, groupName, timeSeriesName);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
					return request.ReadResponseJsonAsync().WithCancellation(token);
			},token);
		}

		public async Task<long> GetOverallTotalAsync(string groupName, string timeSeriesName, CancellationToken token = default(CancellationToken))
		{
			AssertInitialized();

			return await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Get, async (url, timeSeriesStoreName) =>
			{
				var requestUriString = String.Format("{0}/ts/{1}/getTimeSeriesOverallTotal/{2}/{3}", url, timeSeriesStoreName, groupName, timeSeriesName);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
				{
					var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
					return response.Value<long>();
				}
			},token);
		}

		public async Task<List<TimeSeriesView.ServerValue>> GetServersValuesAsync(string groupName, string timeSeriesName, CancellationToken token = default(CancellationToken))
		{
			AssertInitialized();
			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();

			return await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Get, async (url, timeSeriesStoreName) =>
			{
				var requestUriString = String.Format("{0}/getTimeSeriesServersValues/{1}/{2}", url, groupName, timeSeriesName);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
				{
					var response = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
					return response.ToObject<List<TimeSeriesView.ServerValue>>();
				}
			},token);
		}
    }
}
