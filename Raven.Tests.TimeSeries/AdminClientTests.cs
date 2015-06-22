using System;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.TimeSeries;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class AdminClientTests : RavenBaseTimeSeriesTest
	{
		private const string TimeSeriesName = "FooBarTimeSeries";

		[Fact]
		public async Task Should_be_able_to_create_time_series()
		{
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName,createDefaultTimeSeries:false))
			{
				await store.Admin.CreateTimeSeriesAsync(new TimeSeriesDocument(), TimeSeriesName);

				var timeSeriesNames = await store.Admin.GetTimeSeriesNamesAsync();
				timeSeriesNames.Should().HaveCount(1)
									.And.Contain(TimeSeriesName);
			}
		}

		[Fact]
		public async Task Should_be_able_to_create_multiple_time_seriess()
		{
			var expectedClientNames = new[] { TimeSeriesName + "A", TimeSeriesName + "B", TimeSeriesName + "C" };
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName,createDefaultTimeSeries: false))
			{
				var defaultTimeSeriesDocument = new TimeSeriesDocument();
				await store.Admin.CreateTimeSeriesAsync(defaultTimeSeriesDocument, expectedClientNames[0]);
				await store.Admin.CreateTimeSeriesAsync(defaultTimeSeriesDocument, expectedClientNames[1]);
				await store.Admin.CreateTimeSeriesAsync(defaultTimeSeriesDocument, expectedClientNames[2]);

				var timeSeriesNames = await store.Admin.GetTimeSeriesNamesAsync();
				timeSeriesNames.Should().BeEquivalentTo(expectedClientNames);
			}
		}

		[Fact]
		public async Task Should_be_able_to_delete_time_seriess()
		{
			var expectedClientNames = new[] { TimeSeriesName + "A", TimeSeriesName + "C" };
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName, createDefaultTimeSeries: false))
			{
				await store.Admin.CreateTimeSeriesAsync(CreateTimeSeriesDocument(expectedClientNames[0]), expectedClientNames[0]);
				await store.Admin.CreateTimeSeriesAsync(CreateTimeSeriesDocument("TimeSeriesThatWillBeDeleted"), "TimeSeriesThatWillBeDeleted");
				await store.Admin.CreateTimeSeriesAsync(CreateTimeSeriesDocument(expectedClientNames[1]), expectedClientNames[1]);

				await store.Admin.DeleteTimeSeriesAsync("TimeSeriesThatWillBeDeleted", true);

				var timeSeriesNames = await store.Admin.GetTimeSeriesNamesAsync();
				timeSeriesNames.Should().BeEquivalentTo(expectedClientNames);
			}
		}

		[Fact]
		public async Task Should_be_able_to_create_multiple_time_seriess_in_parallel()
		{
			var expectedClientNames = new[] { TimeSeriesName + "A", TimeSeriesName + "B", TimeSeriesName + "C" };
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName, createDefaultTimeSeries: false))
			{
				var defaultTimeSeriesDocument = new TimeSeriesDocument();
				var t1 = store.Admin.CreateTimeSeriesAsync(defaultTimeSeriesDocument, expectedClientNames[0]);
				var t2 = store.Admin.CreateTimeSeriesAsync(defaultTimeSeriesDocument, expectedClientNames[1]);
				var t3 = store.Admin.CreateTimeSeriesAsync(defaultTimeSeriesDocument, expectedClientNames[2]);

				await Task.WhenAll(t1, t2, t3);

				var timeSeriesNames = await store.Admin.GetTimeSeriesNamesAsync();
				timeSeriesNames.Should().BeEquivalentTo(expectedClientNames);
			}
		}


		[Fact]
		public async Task Should_not_be_able_to_create_time_series_with_the_same_name_twice()
		{
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName))
			{
				await store.Admin.CreateTimeSeriesAsync(new TimeSeriesDocument(), TimeSeriesName);

				//invoking create time series with the same name twice should fail
				store.Invoking(c => c.Admin.CreateTimeSeriesAsync(new TimeSeriesDocument(), TimeSeriesName).Wait())
					 .ShouldThrow<InvalidOperationException>();
			}
		}
	}
}
