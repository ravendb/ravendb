using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.TimeSeries;
using Xunit;
using Xunit.Extensions;
using Xunit.Sdk;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesChangeTests : RavenBaseTimeSeriesTest
	{
		private const string TimeSeriesName = "FooBarTimeSeriesStore";
		private const string TimeSeriesName = "FooBarTimeSeries";

		[Theory]
		[InlineData(2)]
		[InlineData(-2)]
		public async Task CountrsReset_should_work(int delta)
		{
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName))
			{
				await store.Admin.CreateTimeSeriesAsync(new TimeSeriesDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/TimeSeries/DataDir", @"~\TimeSeries\Cs1"}
					},
				}, TimeSeriesName);

				const string timeSeriesGroupName = "FooBarGroup";
				await store.ChangeAsync(timeSeriesGroupName, TimeSeriesName, delta);

				var total = await store.GetOverallTotalAsync(timeSeriesGroupName, TimeSeriesName);
				total.Should().Be(delta);
				await store.ResetAsync(timeSeriesGroupName, TimeSeriesName);

				total = await store.GetOverallTotalAsync(timeSeriesGroupName, TimeSeriesName);
				total.Should().Be(0);
			}	
		}

		[Theory]
		[InlineData(2)]
		[InlineData(-2)]
		public async Task CountrsDelete_should_work(int delta)
		{
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName))
			{
				await store.Admin.CreateTimeSeriesAsync(new TimeSeriesDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/TimeSeries/DataDir", @"~\TimeSeries\Cs1"}
					},
				}, TimeSeriesName);

				const string timeSeriesGroupName = "FooBarGroup";
				await store.ChangeAsync(timeSeriesGroupName, TimeSeriesName, delta);

				var total = await store.GetOverallTotalAsync(timeSeriesGroupName, TimeSeriesName);
				total.Should().Be(delta);
				Assert.Throws<InvalidOperationException>(async() => await store.DeleteAsync(timeSeriesGroupName, TimeSeriesName));
			}
		}

		[Fact]
		public async Task TimeSeriesIncrement_should_work()
		{
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName))
			{
				await store.Admin.CreateTimeSeriesAsync(new TimeSeriesDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/TimeSeries/DataDir", @"~\TimeSeries\Cs1"}
					},
				}, TimeSeriesName);

				const string TimeSeriesGroupName = "FooBarGroup12";
				await store.IncrementAsync(TimeSeriesGroupName, TimeSeriesName);

				var total = await store.GetOverallTotalAsync(TimeSeriesGroupName, TimeSeriesName);
				total.Should().Be(1);

				await store.IncrementAsync(TimeSeriesGroupName, TimeSeriesName);

				total = await store.GetOverallTotalAsync(TimeSeriesGroupName, TimeSeriesName);
				total.Should().Be(2);
			}
		}

		[Fact]
		public async Task TimeSeries_change_should_work()
		{
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName))
			{
				await store.Admin.CreateTimeSeriesAsync(new TimeSeriesDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/TimeSeries/DataDir", @"~\TimeSeries\Cs1"}
					},
				}, TimeSeriesName);

				const string TimeSeriesGroupName = "FooBarGroup";
				await store.ChangeAsync(TimeSeriesGroupName, TimeSeriesName, 5);

				var total = await store.GetOverallTotalAsync(TimeSeriesGroupName, TimeSeriesName);
				total.Should().Be(5);

				await store.ChangeAsync(TimeSeriesGroupName, TimeSeriesName, -30);

				total = await store.GetOverallTotalAsync(TimeSeriesGroupName, TimeSeriesName);
				total.Should().Be(-25);
			}
		}
	}
}
