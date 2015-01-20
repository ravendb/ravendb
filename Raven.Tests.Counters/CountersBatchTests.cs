using System.Collections.Generic;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Counters;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Counters
{
	public class CountersBatchTests : RavenBaseCountersTest
	{
		private const string CounterName = "FooBarCounter";

		[Fact]
		public async Task CountersBatch_increment_decrement_should_work()
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
				using (var batchOperation = store.BatchOperation(CounterName,new CountersBatchOptions {BatchSizeLimit = 3}))
				{
					batchOperation.Increment("FooGroup");
					batchOperation.Increment("FooGroup");
					batchOperation.Decrement("FooGroup");
				}

				using (var client = store.NewCounterClient(CounterName))
				{
					var total = await client.Commands.GetOverallTotalAsync("FooGroup");
					total.Should().Be(1);
				}
			}
		}

		[Theory]
		[InlineData(33)]
		[InlineData(100)]
		public async Task CountersBatch_with_multiple_batches_should_work(int countOfOperationsInBatch)
		{
			using (var store = NewRemoteCountersStore())
			{
				await store.CreateCounterAsync(new CountersDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterName);

				using (var counterBatch = store.BatchOperation(CounterName, new CountersBatchOptions { BatchSizeLimit = countOfOperationsInBatch / 2 }))
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
