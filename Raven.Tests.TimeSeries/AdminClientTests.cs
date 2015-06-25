using System;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.TimeSeries;
using Raven.Client.Extensions;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class AdminClientTests : RavenBaseTimeSeriesTest
	{
		private const string TimeSeriesName = "FooBarTimeSeries";

		[Fact]
		public async Task Should_be_able_to_create_time_series()
		{
			using (var store = NewRemoteTimeSeriesStore(createDefaultTimeSeries:false))
			{
				await store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(TimeSeriesName));

				var timeSeriesNames = await store.Admin.GetTimeSeriesNamesAsync();
				timeSeriesNames.Should().HaveCount(1)
									.And.Contain(TimeSeriesName);
			}
		}

		[Fact]
		public async Task Should_be_able_to_create_multiple_time_series()
		{
			var expectedClientNames = new[] { TimeSeriesName + "A", TimeSeriesName + "B", TimeSeriesName + "C" };
			using (var store = NewRemoteTimeSeriesStore(createDefaultTimeSeries: false))
			{
				var defaultTimeSeriesDocument = new TimeSeriesDocument();
				await store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(expectedClientNames[0]));
				await store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(expectedClientNames[1]));
				await store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(expectedClientNames[2]));

				var timeSeriesNames = await store.Admin.GetTimeSeriesNamesAsync();
				timeSeriesNames.Should().BeEquivalentTo(expectedClientNames);
			}
		}

		[Fact]
		public async Task Should_be_able_to_delete_time_series()
		{
			var expectedClientNames = new[] { TimeSeriesName + "A", TimeSeriesName + "C" };
			using (var store = NewRemoteTimeSeriesStore(createDefaultTimeSeries: false))
			{
				await store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(expectedClientNames[0]));
				await store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument("TimeSeriesThatWillBeDeleted"));
				await store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(expectedClientNames[1]));

				await store.Admin.DeleteTimeSeriesAsync("TimeSeriesThatWillBeDeleted", true);

				var timeSeriesNames = await store.Admin.GetTimeSeriesNamesAsync();
				timeSeriesNames.Should().BeEquivalentTo(expectedClientNames);
			}
		}

		[Fact]
		public async Task Should_be_able_to_create_multiple_time_series_in_parallel()
		{
			var expectedClientNames = new[] { TimeSeriesName + "A", TimeSeriesName + "B", TimeSeriesName + "C" };
			using (var store = NewRemoteTimeSeriesStore(createDefaultTimeSeries: false))
			{
				var t1 = store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(expectedClientNames[0]));
				var t2 = store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(expectedClientNames[1]));
				var t3 = store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(expectedClientNames[2]));

				await Task.WhenAll(t1, t2, t3);

				var timeSeriesNames = await store.Admin.GetTimeSeriesNamesAsync();
				timeSeriesNames.Should().BeEquivalentTo(expectedClientNames);
			}
		}


		[Fact]
		public async Task Should_not_be_able_to_create_time_series_with_the_same_name_twice()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				await store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(TimeSeriesName));

				//invoking create time series with the same name twice should fail
				store.Invoking(c => c.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(TimeSeriesName)).Wait())
					 .ShouldThrow<InvalidOperationException>();
			}
		}
	}
}
