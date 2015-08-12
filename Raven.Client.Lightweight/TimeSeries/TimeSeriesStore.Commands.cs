using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.Util;

namespace Raven.Client.TimeSeries
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

			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Put, async (url, timeSeriesName) =>
			{
				var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/types/{2}",
					url, timeSeriesName, type);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Put))
				{
					await request.WriteWithObjectAsync(new TimeSeriesType {Type = type, Fields = fields});
					return await request.ReadResponseJsonAsync().WithCancellation(token);
				}
			}, token);
		}

		public async Task DeleteTypeAsync(string type, CancellationToken token = default(CancellationToken))
		{
			AssertInitialized();
			
			if (string.IsNullOrEmpty(type))
				throw new InvalidOperationException("Prefix cannot be empty");

			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Delete, (url, timeSeriesName) =>
			{
				var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/types/{2}",
					url, timeSeriesName, type);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
				{
					return request.ReadResponseJsonAsync().WithCancellation(token);
				}
			}, token);
		}

		public Task AppendAsync(string type, string key, DateTime at, double value, CancellationToken token = new CancellationToken())
		{
			return AppendAsync(type, key, at, token, value);
		}

		public async Task AppendAsync(string type, string key, DateTime at, CancellationToken token, params double[] values)
		{
			AssertInitialized();

			if (string.IsNullOrEmpty(type) || string.IsNullOrEmpty(key) || at < DateTime.MinValue || values == null || values.Length == 0)
				throw new InvalidOperationException("Append data is invalid");

			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Put, async (url, timeSeriesName) =>
			{
				var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/append/{2}?key={3}",
					url, timeSeriesName, type, key);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Put))
				{
					await request.WriteWithObjectAsync(new TimeSeriesPoint{At = at, Values = values});
					return await request.ReadResponseJsonAsync().WithCancellation(token);
				}
			}, token);
		}

		public Task AppendAsync(string type, string key, DateTime at, double[] values, CancellationToken token = new CancellationToken())
		{
			return AppendAsync(type, key, at, token, values);
		}

		public async Task DeleteAsync(string type, string key, CancellationToken token = new CancellationToken())
		{
			AssertInitialized();

			if (string.IsNullOrEmpty(type) || string.IsNullOrEmpty(key))
				throw new InvalidOperationException("Data is invalid");

			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Post, (url, timeSeriesName) =>
			{
				var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/delete/{2}?key={3}",
					url, timeSeriesName, type, key);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
				{
					return request.ReadResponseJsonAsync().WithCancellation(token);
				}
			}, token);
		}

		public async Task DeleteRangeAsync(string type, string key, DateTime start, DateTime end, CancellationToken token = new CancellationToken())
		{
			AssertInitialized();

			if (string.IsNullOrEmpty(type) || string.IsNullOrEmpty(key))
				throw new InvalidOperationException("Data is invalid");

			if (start > end)
				throw new InvalidOperationException("start cannot be greater than end");

			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Post, (url, timeSeriesName) =>
			{
				var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/delete-range/{2}?key={3}start={4}&end={5}",
					url, timeSeriesName, type, key, start.Ticks, end.Ticks);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
				{
					return request.ReadResponseJsonAsync().WithCancellation(token);
				}
			}, token);
		}
    }
}
