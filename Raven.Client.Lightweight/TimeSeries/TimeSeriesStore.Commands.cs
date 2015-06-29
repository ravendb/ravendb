using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;

namespace Raven.Client.TimeSeries
{
	public partial class TimeSeriesStore
    {
		public Task AppendAsync(string key, DateTime time, double value, CancellationToken token = new CancellationToken())
		{
			return AppendAsync(key, time, token, value);
		}

		public async Task AppendAsync(string key, DateTime time, CancellationToken token, params double[] values)
		{
			AssertInitialized();
			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Post, (url, timeSeriesName) =>
			{
				var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/append/{2}?time={3}&{4}",
					url, timeSeriesName, key, time.Ticks, string.Join("&", values.Select(v => "values=" + v)));
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
				{
					return request.ReadResponseJsonAsync().WithCancellation(token);
				}
			}, token); 
		}

		public Task AppendAsync(string key, DateTime time, double[] values, CancellationToken token = new CancellationToken())
		{
			return AppendAsync(key, time, token, values);
		}

		public Task DeleteAsync(string key, CancellationToken token = new CancellationToken())
		{
			throw new NotImplementedException();
		}

		public Task DeleteRangeAsync(string key, DateTime start, DateTime end, CancellationToken token = new CancellationToken())
		{
			throw new NotImplementedException();
		}
    }
}
