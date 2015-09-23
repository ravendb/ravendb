using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Util;
using Raven.Client.TimeSeries.Operations;
using Raven.Json.Linq;

namespace Raven.Client.TimeSeries
{
    public partial class TimeSeriesStore
    {
		public class TimeSeriesStoreAdvancedOperations
		{
			private readonly TimeSeriesStore parent;

			internal TimeSeriesStoreAdvancedOperations(TimeSeriesStore parent)
			{
				this.parent = parent;
			}

			public TimeSeriesBatchOperation NewBatch(TimeSeriesBatchOptions options = null)
			{
				if (parent.Name == null)
					throw new ArgumentException("Time series isn't set!");

				parent.AssertInitialized();

				return new TimeSeriesBatchOperation(parent, parent.Name, options);
			}

			public async Task<TimeSeriesKey[]> GetKeys(string type, CancellationToken token =  default(CancellationToken))
			{
				parent.AssertInitialized();

				await parent.ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);
				return await parent.ReplicationInformer.ExecuteWithReplicationAsync(parent.Url, HttpMethods.Get, async (url, timeSeriesName) =>
				{
					var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/keys/{2}",
						url, timeSeriesName, type);
					using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
					{
						var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
						return result.JsonDeserialization<TimeSeriesKey[]>();
					}
				}, token).ConfigureAwait(false);
			}

			public async Task<TimeSeriesPoint[]> GetPoints(string type, string key, int skip = 0, int take = 20, DateTimeOffset? start = null, DateTimeOffset? end = null, CancellationToken token =  default(CancellationToken))
			{
				parent.AssertInitialized();

				await parent.ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);
				return await parent.ReplicationInformer.ExecuteWithReplicationAsync(parent.Url, HttpMethods.Get, async (url, timeSeriesName) =>
				{
					var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/points/{2}?key={3}&skip={4}&take={5}&start={6}&end={7}",
						url, timeSeriesName, type, Uri.EscapeDataString(key), skip, take, start, end);
					using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
					{
						var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
						return result.JsonDeserialization<TimeSeriesPoint[]>();
					}
				}, token).ConfigureAwait(false);
			}

			public async Task<TimeSeriesType[]> GetTypes(CancellationToken token = default(CancellationToken))
			{
				parent.AssertInitialized();

				await parent.ReplicationInformer.UpdateReplicationInformationIfNeededAsync().ConfigureAwait(false);
				return await parent.ReplicationInformer.ExecuteWithReplicationAsync(parent.Url, HttpMethods.Get, async (url, timeSeriesName) =>
				{
					var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/types", url, timeSeriesName);
					using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
					{
						var result = await request.ReadResponseJsonAsync().WithCancellation(token).ConfigureAwait(false);
						return result.JsonDeserialization<TimeSeriesType[]>();
					}
				}, token).ConfigureAwait(false);
			}
		}
    }
}