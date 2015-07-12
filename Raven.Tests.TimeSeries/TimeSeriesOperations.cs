// -----------------------------------------------------------------------
//  <copyright file="TimeSeriesOperations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.TimeSeries;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesOperations : RavenBaseTimeSeriesTest
	{
		[Fact]
		public async Task SimpleAppend()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.CreatePrefixConfigurationAsync("-Simple", 1);
				await store.CreatePrefixConfigurationAsync("-ForValues", 4);

				await store.AppendAsync("-ForValues", "Time", DateTime.Now, new[] { 3D, 4D, 5D, 6D });
				await store.AppendAsync("-Simple", "Is", DateTime.Now, 3D);
				await store.AppendAsync("-Simple", "Money", DateTime.Now, 3D);
				
				var cancellationToken = new CancellationToken();
				await store.AppendAsync("-Simple", "Is", DateTime.Now, 3456D, cancellationToken);
				await store.AppendAsync("-ForValues", "Time", DateTime.Now, new[] { 23D, 4D, 5D, 6D }, cancellationToken);
				await store.AppendAsync("-ForValues", "Time", DateTime.Now, cancellationToken, 33D, 4D, 5D, 6D);

				var stats = await store.GetTimeSeriesStatsAsync(cancellationToken);
				Assert.Equal(2, stats.PrefixesCount);
				Assert.Equal(3, stats.KeysCount);
				Assert.Equal(6, stats.ValuesCount);
			}
		}

		[Fact]
		public async Task ShouldNotAllowOverwritePrefix()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.CreatePrefixConfigurationAsync("-Simple", 1);
				var exception = await AssertAsync.Throws<ErrorResponseException>(async () => await store.CreatePrefixConfigurationAsync("-Simple", 2));
				Assert.Contains("System.InvalidOperationException: Prefix -Simple is already created", exception.Message);

				var stats = await store.GetTimeSeriesStatsAsync();
				Assert.Equal(1, stats.PrefixesCount);
			}
		}

		[Fact]
		public async Task SimpleAppend_ShouldFailIfTwoKeysAsDifferentValuesLength()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.CreatePrefixConfigurationAsync("-Simple", 1);
				await store.AppendAsync("-Simple", "Time", DateTime.Now, 3D);

				var exception = await AssertAsync.Throws<ErrorResponseException>(async () => await store.AppendAsync("-Simple", "Time", DateTime.Now, new[] { 3D, 4D, 5D, 6D }));
				Assert.Contains("System.ArgumentOutOfRangeException: Appended values should be the same length the series values length which is 1 and not 4", exception.Message);

				var stats = await store.GetTimeSeriesStatsAsync();
				Assert.Equal(1, stats.PrefixesCount);
				Assert.Equal(1, stats.KeysCount);
				Assert.Equal(1, stats.ValuesCount);
			}
		}

		[Fact]
		public async Task AddAndDeletePrefix_ShouldThrowIfPrefixHasData()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.CreatePrefixConfigurationAsync("-Simple", 1);
				await store.AppendAsync("-Simple", "Is", DateTime.Now, 3D);

				var exception = await AssertAsync.Throws<ErrorResponseException>(async () => await store.DeletePrefixConfigurationAsync("-Simple"));
				Assert.Contains("System.InvalidOperationException: Cannot delete prefix since there is associated data to it", exception.Message);

				var stats = await store.GetTimeSeriesStatsAsync();
				Assert.Equal(1, stats.PrefixesCount);
				Assert.Equal(1, stats.KeysCount);
				Assert.Equal(1, stats.ValuesCount);
			}
		}



		[Fact]
		public async Task AddAndDeletePrefix()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.CreatePrefixConfigurationAsync("-Simple", 1);
				await store.AppendAsync("-Simple", "Is", DateTime.Now, 3D);
				await store.DeleteAsync("-Simple", "Is");
				await store.DeletePrefixConfigurationAsync("-Simple");

				var stats = await store.GetTimeSeriesStatsAsync();
				Assert.Equal(0, stats.PrefixesCount);
				Assert.Equal(0, stats.KeysCount);
				Assert.Equal(0, stats.ValuesCount);
			}
		}


		[Fact]
		public async Task DeleteRange()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.CreatePrefixConfigurationAsync("-Simple", 1);
				
				var start = new DateTime(2015, 1, 1);
				for (int i = 0; i < 12; i++)
				{
					await store.AppendAsync("-Simple", "Time", start.AddHours(i), i + 3D);
				}
				var stats = await store.GetTimeSeriesStatsAsync();
				Assert.Equal(1, stats.PrefixesCount);
				Assert.Equal(1, stats.KeysCount);
				Assert.Equal(12, stats.ValuesCount);

				await store.DeleteRangeAsync("-Simple", "Time", start.AddHours(3), start.AddHours(7));
				stats = await store.GetTimeSeriesStatsAsync();
				Assert.Equal(1, stats.PrefixesCount);
				Assert.Equal(1, stats.KeysCount);
				Assert.Equal(7, stats.ValuesCount);
			}
		}

		[Fact]
		public async Task AdvancedAppend()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.CreatePrefixConfigurationAsync("-Simple", 1);
				await store.CreatePrefixConfigurationAsync("-ForValues", 4);

				using (var batch = store.Advanced.NewBatch(new TimeSeriesBatchOptions { }))
				{
					for (int i = 0; i < 1888; i++)
					{
						batch.ScheduleAppend("-Simple", "Is", DateTime.Now, 3D);
						batch.ScheduleAppend("-Simple", "Money", DateTime.Now, 13D);
						batch.ScheduleAppend("-ForValues", "Time", DateTime.Now, 3D, 4D, 5D, 6D);
					}
					await batch.FlushAsync();
				}

				var stats = await store.GetTimeSeriesStatsAsync();
				Assert.Equal(2, stats.PrefixesCount);
				Assert.Equal(3, stats.KeysCount);
				Assert.Equal(1888 * 3, stats.ValuesCount);

				WaitForUserToContinueTheTest(startPage: "/studio/index.html#timeseries/series?prefix=-Simple&key=Money&timeseries=SeriesName-1");
			}
		}

		[Fact]
		public async Task GetKeys()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.CreatePrefixConfigurationAsync("-Simple", 1);
				await store.CreatePrefixConfigurationAsync("-ForValues", 4);

				await store.AppendAsync("-ForValues", "Time", DateTime.Now, new[] { 3D, 4D, 5D, 6D });
				await store.AppendAsync("-Simple", "Is", DateTime.Now, 3D);
				await store.AppendAsync("-Simple", "Money", DateTime.Now, 3D);

				var cancellationToken = new CancellationToken();
				await store.AppendAsync("-Simple", "Is", DateTime.Now.AddHours(1), 3456D, cancellationToken);
				await store.AppendAsync("-ForValues", "Time", DateTime.Now.AddHours(1), new[] { 23D, 4D, 5D, 6D }, cancellationToken);
				await store.AppendAsync("-ForValues", "Time", DateTime.Now.AddHours(2), cancellationToken, 33D, 4D, 5D, 6D);
				await store.AppendAsync("-ForValues", "Time", DateTime.Now.AddHours(3), cancellationToken, 33D, 4D, 5D, 6D);

				var keys = await store.Advanced.GetKeys(cancellationToken);
				Assert.Equal(3, keys.Length);
				
				var time = keys[0];
				Assert.Equal("-ForValues", time.Prefix);
				Assert.Equal(4, time.ValueLength);
				Assert.Equal("Time", time.Key);
				Assert.Equal(4, time.PointsCount);

				var _is = keys[1];
				Assert.Equal("-Simple", _is.Prefix);
				Assert.Equal(1, _is.ValueLength);
				Assert.Equal("Is", _is.Key);
				Assert.Equal(2, _is.PointsCount);

				var money = keys[2];
				Assert.Equal("-Simple", money.Prefix);
				Assert.Equal(1, money.ValueLength);
				Assert.Equal("Money", money.Key);
				Assert.Equal(1, money.PointsCount);
			}
		}
	}
}