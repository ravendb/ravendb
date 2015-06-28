/*
using FluentAssertions;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.TimeSeries
{
    public class TimeSeriesStatsTests : RavenBaseTimeSeriesTest
	{
	    [Fact]
	    public async Task Fetching_time_series_stats_should_work()
	    {
		    using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName))
		    {
				await store.IncrementAsync("group1", "f");
				await store.IncrementAsync("group2", "f");
				await store.DecrementAsync("group2", "g");
				await store.IncrementAsync("group3", "f");
				
				var stats = await store.GetTimeSeriesStatsAsync();

				stats.GroupsCount.Should().Be(3);
				stats.TimeSeriesCount.Should().Be(4);
			}
	    }
	}
}
*/
