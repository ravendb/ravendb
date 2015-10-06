using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Counters
{
    public class CounterStatsTests : RavenBaseCountersTest
	{
	    [Fact]
	    public async Task Fetching_counter_storage_stats_should_work()
	    {
		    using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
		    {
				await store.IncrementAsync("group1", "f");
				await store.IncrementAsync("group2", "f");
				await store.DecrementAsync("group2", "g");
				await store.IncrementAsync("group3", "f");
				
				var stats = await store.GetCounterStatsAsync();

                Assert.Equal(3, stats.GroupsCount);
                Assert.Equal(4, stats.CountersCount);
			}
	    }
	}
}
