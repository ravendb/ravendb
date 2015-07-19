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

				await parent.ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
				return await parent.ReplicationInformer.ExecuteWithReplicationAsync(parent.Url, HttpMethods.Get, async (url, timeSeriesName) =>
				{
					var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/{2}/keys",
						url, timeSeriesName, type);
					using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
					{
						var result = await request.ReadResponseJsonAsync().WithCancellation(token);
						return result.JsonDeserialization<TimeSeriesKey[]>();
					}
				}, token);
			}

			public async Task<TimeSeriesType[]> GetTypes(CancellationToken token = default(CancellationToken))
			{
				parent.AssertInitialized();

				await parent.ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
				return await parent.ReplicationInformer.ExecuteWithReplicationAsync(parent.Url, HttpMethods.Get, async (url, timeSeriesName) =>
				{
					var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/types", url, timeSeriesName);
					using (var request = parent.CreateHttpJsonRequest(requestUriString, HttpMethods.Get))
					{
						var result = await request.ReadResponseJsonAsync().WithCancellation(token);
						return result.JsonDeserialization<TimeSeriesType[]>();
					}
				}, token);
			}
		}
    }
}