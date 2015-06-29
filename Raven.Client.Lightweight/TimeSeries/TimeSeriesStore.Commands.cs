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
		public async Task AppendAsync(string key, DateTime time, double value, CancellationToken token = new CancellationToken())
		{
			AssertInitialized();

			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			throw new NotImplementedException();
		
			/*await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Post, (url, timeSeriesName) =>
			{

			}, token);*/
		}

		public Task AppendAsync(string key, DateTime time, CancellationToken token, params double[] values)
		{
			throw new NotImplementedException();
		}

		public Task AppendAsync(string key, DateTime time, double[] values, CancellationToken token = new CancellationToken())
		{
			throw new NotImplementedException();
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
