using FluentAssertions;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Counters
{
    public class CounterStatsTests : RavenBaseCountersTest
	{
	    [Fact]
	    public async Task Fetching_counter_storage_stats_should_work()
	    {
		    using (var store = NewRemoteCountersStore())
		    {
				await store.CreateCounterStorageAsync(CreateCounterStorageDocument("c1"), "c1");

				using (var client = store.NewCounterClient("c1"))
				{
					await client.Commands.IncrementAsync("group1","f");
					await client.Commands.IncrementAsync("group2", "f");
					await client.Commands.DecrementAsync("group2", "g");
					await client.Commands.IncrementAsync("group3", "f");

					var stats = await client.Stats.GetCounterStatsAsync();
					
					stats.CountersCount.Should().Be(4);
					stats.GroupsCount.Should().Be(3);
				}
		    }
	    }
	}
}
