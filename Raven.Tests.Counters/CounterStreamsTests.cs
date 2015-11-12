using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Counters;
using Xunit;

namespace Raven.Tests.Counters
{
	public class CounterStreamsTests : RavenBaseCountersTest
	{
		[Fact]
		public async Task Streaming_counter_summaries_per_group_should_fetch_only_group_counters()
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
		public async Task Streaming_counter_summaries_by_prefix_should_fetch_counters_with_relevant_prefix()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				await store.IncrementAsync("g1", "AAA");
				await store.IncrementAsync("g1", "AAB");
				await store.IncrementAsync("g1", "AAC");
				await store.IncrementAsync("g1", "BBA");
				await store.IncrementAsync("g1", "BBB");
				await store.IncrementAsync("g2", "c1");
				await store.IncrementAsync("g2", "c2");

				using (var enumerator = await store.Stream.CounterSummariesByPrefix("g1", "AA"))
				{
					var summaries = new List<CounterSummary>();
					while (await enumerator.MoveNextAsync())
						summaries.Add(enumerator.Current);

					Assert.Equal(3, summaries.Count);
					Assert.True(summaries.Any(x => x.CounterName == "AAA" && x.GroupName == "g1" && x.Total == 1));
					Assert.True(summaries.Any(x => x.CounterName == "AAB" && x.GroupName == "g1" && x.Total == 1));
					Assert.True(summaries.Any(x => x.CounterName == "AAC" && x.GroupName == "g1" && x.Total == 1));
				}

				using (var enumerator = await store.Stream.CounterSummariesByPrefix("g1","BB"))
				{
					var summaries = new List<CounterSummary>();
					while (await enumerator.MoveNextAsync())
						summaries.Add(enumerator.Current);

					Assert.Equal(2, summaries.Count);
					Assert.True(summaries.Any(x => x.CounterName == "BBA" && x.GroupName == "g1" && x.Total == 1));
					Assert.True(summaries.Any(x => x.CounterName == "BBB" && x.GroupName == "g1" && x.Total == 1));
				}

			

			}
		}

		[Fact]
		public async Task Streaming_counter_summaries_with_empty_prefix_should_fetch_all_group_counters()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				await store.IncrementAsync("g1", "c1");
				await store.IncrementAsync("g1", "c2");
				await store.IncrementAsync("g1", "c2");
				await store.IncrementAsync("g2", "c1");

				using (var enumerator = await store.Stream.CounterSummariesByPrefix("g1",null))
				{
					var summaries = new List<CounterSummary>();
					while (await enumerator.MoveNextAsync())
						summaries.Add(enumerator.Current);

					Assert.Equal(2, summaries.Count);
					Assert.True(summaries.Any(x => x.CounterName == "c1" && x.GroupName == "g1" && x.Total == 1));
					Assert.True(summaries.Any(x => x.CounterName == "c2" && x.GroupName == "g1" && x.Total == 2));
				}

				using (var enumerator = await store.Stream.CounterSummariesByPrefix("g2",null))
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
		public async Task Streaming_counter_summaries_for_all_groups_should_fetch_all_groups_and_counters()
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

		[Fact]
		public async Task Streaming_counter_groups_should_fetch_all_counter_groups()
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

				using (var enumerator = await store.Stream.CounterGroups())
				{
					var groups = new List<CounterGroup>();
					while (await enumerator.MoveNextAsync())
						groups.Add(enumerator.Current);

					Assert.Equal(3, groups.Count);
					Assert.True(groups.Any(x => x.Name == "g1" && x.Count == 2));
					Assert.True(groups.Any(x => x.Name == "g2" && x.Count == 1));
					Assert.True(groups.Any(x => x.Name == "g3" && x.Count == 1));
				}
			}
		}			
	}
}
