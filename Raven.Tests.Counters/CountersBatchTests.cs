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
		private const string CounterStorageName = "FooBarCounter";

		[Fact]
		public async Task CountersBatch_increment_decrement_should_work()
		{
			using (var store = NewRemoteCountersStore())
			{
				await store.CreateCounterStorageAsync(new CounterStorageDocument()
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName);
				using (var batchOperation = store.BatchOperation(CounterStorageName,new CountersBatchOptions {BatchSizeLimit = 3}))
				{
					batchOperation.Increment("FooGroup","FooCounter");
					batchOperation.Increment("FooGroup", "FooCounter");
					batchOperation.Decrement("FooGroup", "FooCounter");
				}

				using (var client = store.NewCounterClient(CounterStorageName))
				{
					var total = await client.Commands.GetOverallTotalAsync("FooGroup", "FooCounter");
					total.Should().Be(1);
				}
			}
		}

		[Fact]
		public async Task CountersBatch_increment_decrement_with_different_counters_in_the_batch_should_work()
		{
			using (var store = NewRemoteCountersStore())
			{
				await store.CreateCounterStorageAsync(new CounterStorageDocument()
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName);
				using (var batchOperation = store.BatchOperation(CounterStorageName, new CountersBatchOptions { BatchSizeLimit = 3 }))
				{
					batchOperation.Increment("FooGroup", "FooCounter");
					batchOperation.Increment("FooGroup", "FooCounter");
					batchOperation.Decrement("FooGroup", "FooCounter");

					batchOperation.Increment("FooGroup", "FooCounter2");
					batchOperation.Decrement("FooGroup", "FooCounter2");
					batchOperation.Decrement("FooGroup", "FooCounter2");
				}

				using (var client = store.NewCounterClient(CounterStorageName))
				{
					var total = await client.Commands.GetOverallTotalAsync("FooGroup", "FooCounter2");
					total.Should().Be(-1);

					total = await client.Commands.GetOverallTotalAsync("FooGroup", "FooCounter");
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
				await store.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName);

				using (var counterBatch = store.BatchOperation(CounterStorageName, new CountersBatchOptions { BatchSizeLimit = countOfOperationsInBatch / 2 }))
				{
					int x = 0;
					for (int i = 0; i < countOfOperationsInBatch; i++)
					{
						counterBatch.Increment("FooGroup", "FooCounter");
						x++;
					}
				}

				using (var client = store.NewCounterClient(CounterStorageName))
				{
					var total = await client.Commands.GetOverallTotalAsync("FooGroup", "FooCounter");
					total.Should().Be(countOfOperationsInBatch);
				}
			}

		}
	}
}
