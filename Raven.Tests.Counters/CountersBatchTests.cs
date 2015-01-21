using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.Counters;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Counters
{
	public class CountersBatchTests : RavenBaseCountersTest
	{
		private const string CounterStorageName1 = "FooBarCounter1";

		private const string CounterStorageName2 = "FooBarCounter2";

		private const string CounterStorageName3 = "FooBarCounter3";

		[Fact]
		public async Task CountersBatch_increment_decrement_should_work()
		{
			using (var store = NewRemoteCountersStore())
			{
				await store.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, CounterStorageName1);
				using (var batchOperation = store.Advanced.NewBatch(CounterStorageName1,
														new CountersBatchOptions {BatchSizeLimit = 3}))
				{
					batchOperation.ScheduleIncrement("FooGroup","FooCounter");
					batchOperation.ScheduleIncrement("FooGroup", "FooCounter");
					batchOperation.ScheduleDecrement("FooGroup", "FooCounter");
				}

				using (var client = store.NewCounterClient(CounterStorageName1))
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
				}, CounterStorageName1);
				using (var batchOperation = store.Advanced.NewBatch(CounterStorageName1, new CountersBatchOptions { BatchSizeLimit = 3 }))
				{
					batchOperation.ScheduleIncrement("FooGroup", "FooCounter");
					batchOperation.ScheduleIncrement("FooGroup", "FooCounter");
					batchOperation.ScheduleDecrement("FooGroup", "FooCounter");

					batchOperation.ScheduleIncrement("FooGroup", "FooCounter2");
					batchOperation.ScheduleDecrement("FooGroup", "FooCounter2");
					batchOperation.ScheduleDecrement("FooGroup", "FooCounter2");
				}

				using (var client = store.NewCounterClient(CounterStorageName1))
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
				}, CounterStorageName1);

				using (var counterBatch = store.Advanced.NewBatch(CounterStorageName1, new CountersBatchOptions { BatchSizeLimit = countOfOperationsInBatch / 2 }))
				{
					int x = 0;
					for (int i = 0; i < countOfOperationsInBatch; i++)
					{
						counterBatch.ScheduleIncrement("FooGroup", "FooCounter");
						x++;
					}
				}

				using (var client = store.NewCounterClient(CounterStorageName1))
				{
					var total = await client.Commands.GetOverallTotalAsync("FooGroup", "FooCounter");
					total.Should().Be(countOfOperationsInBatch);
				}
			}
		}

		[Theory]
		//[InlineData(33)]
		[InlineData(10)]
		public async Task CountersBatch_using_batch_store_should_work(int countOfOperationsInBatch)
		{
			using (var store = NewRemoteCountersStore())
			{
				await store.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\" + CounterStorageName1}
					},
				}, CounterStorageName1);

				store.Batch[CounterStorageName1].Options.BatchSizeLimit = countOfOperationsInBatch/2;
				int x = 0;
				for (int i = 0; i < countOfOperationsInBatch; i++)
				{
					store.Batch[CounterStorageName1].ScheduleIncrement("FooGroup", "FooCounter");
					x++;
				}

				await store.Batch[CounterStorageName1].FlushAsync();
				using (var client = store.NewCounterClient(CounterStorageName1))
				{
					var total = await client.Commands.GetOverallTotalAsync("FooGroup", "FooCounter");
					total.Should().Be(countOfOperationsInBatch);
				}
			}
		}
	}
}
