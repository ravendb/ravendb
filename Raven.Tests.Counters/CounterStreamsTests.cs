using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Client.Counters;
using Xunit;

namespace Raven.Tests.Counters
{
	public class CounterStreamsTests : RavenBaseCountersTest
	{
		[Fact]
		public async Task Streaming_counter_summaries_per_group_should_work()
		{						
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				await store.IncrementAsync("g1", "c1");
				await store.IncrementAsync("g1", "c2");
				await store.IncrementAsync("g1", "c2");
				await store.IncrementAsync("g2", "c1");

				using (var enumerator = await store.Stream.CounterSummaries("g1"))
				{
					var summaries = new List<CounterSummary>();
					while (await enumerator.MoveNextAsync())
						summaries.Add(enumerator.Current);

					Assert.Equal(2, summaries.Count);
					Assert.True(summaries.Any(x => x.CounterName == "c1" && x.GroupName == "g1" && x.Total == 1));
					Assert.True(summaries.Any(x => x.CounterName == "c2" && x.GroupName == "g1" && x.Total == 2));
				}

				using (var enumerator = await store.Stream.CounterSummaries("g2"))
				{
					var summaries = new List<CounterSummary>();
					while (await enumerator.MoveNextAsync())
						summaries.Add(enumerator.Current);

					Assert.Equal(1, summaries.Count);
					Assert.True(summaries.Any(x => x.CounterName == "c1" && x.GroupName == "g2" && x.Total == 1));
				}

			}
		}

		[Fact]
		public async Task Streaming_counter_summaries_for_all_groups_should_work()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				await store.IncrementAsync("g1", "c1");
				await store.IncrementAsync("g1", "c2");
				await store.IncrementAsync("g1", "c2");
				await store.IncrementAsync("g2", "c1");
				await store.IncrementAsync("g3", "c1");
				await store.IncrementAsync("g3", "c1");
				await store.IncrementAsync("g3", "c1");

				using (var enumerator = await store.Stream.CounterSummaries())
				{
					var summaries = new List<CounterSummary>();
					while (await enumerator.MoveNextAsync())
						summaries.Add(enumerator.Current);

					Assert.Equal(4, summaries.Count);
					Assert.True(summaries.Any(x => x.CounterName == "c1" && x.GroupName == "g1" && x.Total == 1));
					Assert.True(summaries.Any(x => x.CounterName == "c2" && x.GroupName == "g1" && x.Total == 2));
					Assert.True(summaries.Any(x => x.CounterName == "c1" && x.GroupName == "g2" && x.Total == 1));
					Assert.True(summaries.Any(x => x.CounterName == "c1" && x.GroupName == "g3" && x.Total == 3));
				}
			}
		}

	}
}
