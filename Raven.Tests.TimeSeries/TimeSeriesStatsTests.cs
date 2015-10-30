using System;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.TimeSeries
{
    public class TimeSeriesStatsTests : RavenBaseTimeSeriesTest
    {
        [Fact]
        public async Task Fetching_time_series_stats_should_work()
        {
            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                await store.AppendAsync("Simple", "Time", DateTimeOffset.Now, 3d);
                await store.AppendAsync("Simple", "Time", DateTimeOffset.Now.AddHours(1), 4d);
                await store.AppendAsync("Simple", "Is", DateTimeOffset.Now, 5d);
                await store.AppendAsync("Simple", "Money", DateTimeOffset.Now, 6d);
                await store.AppendAsync("Simple", "Money", DateTimeOffset.Now.AddMilliseconds(1), 7d);
                await store.AppendAsync("Simple", "Money", DateTimeOffset.Now.AddMilliseconds(2), 8d);
                
                var stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(3, stats.KeysCount);
                Assert.Equal(6, stats.PointsCount);
            }
        }

        [Fact]
        public async Task AppendOnTheSameTime_ShouldOverwriteExistingPoint()
        {
            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                var now = DateTimeOffset.Now;
                await store.AppendAsync("Simple", "Time", now, 3d);
                await store.AppendAsync("Simple", "Time", now.AddHours(1), 4d);
                await store.AppendAsync("Simple", "Is", now, 5d);
                await store.AppendAsync("Simple", "Money", now, 6d);
                await store.AppendAsync("Simple", "Money", now, 7d);
                await store.AppendAsync("Simple", "Money", now, 8d);

                var stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(3, stats.KeysCount);
                Assert.Equal(4, stats.PointsCount);
            }
        }
    }
}
