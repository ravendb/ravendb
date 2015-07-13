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
		public async Task CreatePrefixConfigurationAsync(string prefix, byte valueLength, CancellationToken token = new CancellationToken())
		{
			AssertInitialized();

			if (string.IsNullOrEmpty(prefix))
				throw new InvalidOperationException("Prefix cannot be empty");

			if (prefix.StartsWith("-") == false)
				throw new InvalidOperationException("Prefix must start with '-' char");

			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Post, (url, timeSeriesName) =>
			{
				var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/prefix-create/{2}?valueLength={3}",
					url, timeSeriesName, prefix, valueLength);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
				{
					return request.ReadResponseJsonAsync().WithCancellation(token);
				}
			}, token);
		}

		public async Task DeletePrefixConfigurationAsync(string prefix, CancellationToken token = default(CancellationToken))
		{
			AssertInitialized();
			
			if (string.IsNullOrEmpty(prefix))
				throw new InvalidOperationException("Prefix cannot be empty");

			if (prefix.StartsWith("-") == false)
				throw new InvalidOperationException("Prefix must start with '-' char");

			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Delete, (url, timeSeriesName) =>
			{
				var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/prefix-delete/{2}",
					url, timeSeriesName, prefix);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
				{
					return request.ReadResponseJsonAsync().WithCancellation(token);
				}
			}, token);
		}

		public Task AppendAsync(string prefix, string key, DateTime at, double value, CancellationToken token = new CancellationToken())
		{
			return AppendAsync(prefix, key, at, token, value);
		}

		public async Task AppendAsync(string prefix, string key, DateTime at, CancellationToken token, params double[] values)
		{
			AssertInitialized();

			if (string.IsNullOrEmpty(prefix) || string.IsNullOrEmpty(key) || at < DateTime.MinValue || values == null || values.Length == 0)
				throw new InvalidOperationException("Append data is invalid");

			if (prefix.StartsWith("-") == false) 
				throw new InvalidOperationException("Prefix must start with '-' char");

			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Post, async (url, timeSeriesName) =>
			{
				var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/append/{2}/{3}",
					url, timeSeriesName, prefix, key);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Post))
				{
					await request.WriteWithObjectAsync(new TimeSeriesPoint{At = at, Values = values});
					return await request.ReadResponseJsonAsync().WithCancellation(token);
				}
			}, token);
		}

		public Task AppendAsync(string prefix, string key, DateTime at, double[] values, CancellationToken token = new CancellationToken())
		{
			return AppendAsync(prefix, key, at, token, values);
		}

		public async Task DeleteAsync(string prefix, string key, CancellationToken token = new CancellationToken())
		{
			AssertInitialized();

			if (string.IsNullOrEmpty(prefix) || string.IsNullOrEmpty(key))
				throw new InvalidOperationException("Data is invalid");

			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Post, (url, timeSeriesName) =>
			{
				var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/delete/{2}/{3}",
					url, timeSeriesName, prefix, key);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
				{
					return request.ReadResponseJsonAsync().WithCancellation(token);
				}
			}, token);
		}

		public async Task DeleteRangeAsync(string prefix, string key, DateTime start, DateTime end, CancellationToken token = new CancellationToken())
		{
			AssertInitialized();

			if (string.IsNullOrEmpty(prefix) || string.IsNullOrEmpty(key))
				throw new InvalidOperationException("Data is invalid");

			if (start > end)
				throw new InvalidOperationException("start cannot be greater than end");

			await ReplicationInformer.UpdateReplicationInformationIfNeededAsync();
			await ReplicationInformer.ExecuteWithReplicationAsync(Url, HttpMethods.Post, (url, timeSeriesName) =>
			{
				var requestUriString = string.Format(CultureInfo.InvariantCulture, "{0}ts/{1}/deleteRange/{2}/{3}?start={4}&end={5}",
					url, timeSeriesName, prefix, key, start.Ticks, end.Ticks);
				using (var request = CreateHttpJsonRequest(requestUriString, HttpMethods.Delete))
				{
					return request.ReadResponseJsonAsync().WithCancellation(token);
				}
			}, token);
		}
    }
}
