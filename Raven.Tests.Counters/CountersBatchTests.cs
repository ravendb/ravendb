using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Counters;
using Raven.Client.Linq;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Counters
{
	public class CountersBatchTests : RavenBaseCountersTest
	{
		private const string CounterName = "FooBarCounter";

		[Fact]
		public async Task CountersBatch_should_work()
		{
			using (var store = NewRemoteCountersStore())
			{
				await store.CreateCounterAsync(new CountersDocument()
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterName);
				using (var counterBatch = store.BatchOperation(CounterName,new CountersBatchOptions {BatchSizeLimit = 3}))
				{
					counterBatch.Increment("FooGroup");
					counterBatch.Increment("FooGroup");
					counterBatch.Decrement("FooGroup");
				}

				using (var client = store.NewCounterClient(CounterName))
				{
					var total = await client.Commands.GetOverallTotalAsync("FooGroup");
					total.Should().Be(1);
				}
			}
		}

		[Theory]
		//[InlineData(10)]
		[InlineData(100000)]
		public async Task CountersBatch_with_multiple_batches_should_work(int countOfOperationsInBatch)
		{
			using (var store = NewRemoteCountersStore())
			{
				await store.CreateCounterAsync(new CountersDocument()
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterName);
				using (var counterBatch = store.BatchOperation(CounterName, new CountersBatchOptions { BatchSizeLimit = countOfOperationsInBatch }))
				{
					int x = 0;
					for (int i = 0; i < countOfOperationsInBatch; i++)
					{
						counterBatch.Increment("FooGroup");
						x++;
					}
				}

				using (var client = store.NewCounterClient(CounterName))
				{
					var total = await client.Commands.GetOverallTotalAsync("FooGroup");
					total.Should().Be(countOfOperationsInBatch);
				}
			}

		}
	}
}
