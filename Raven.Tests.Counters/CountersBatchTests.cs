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
		[InlineData(50)]
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

				using (var counterBatch = store.Advanced.NewBatch(CounterStorageName1,
					new CountersBatchOptions { BatchSizeLimit = countOfOperationsInBatch / 2 }))
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

		[Fact]
		public async Task CounterBatch_using_batch_store_for_default_store_should_work()
		{
			using (var store = NewRemoteCountersStore())
			{
				store.Batch.ScheduleIncrement("FooGroup", "FooCounter");//schedule increment for default counter storage
				await store.Batch.FlushAsync();

				//with counter storage name null - client is opened for default counter
				using (var client = store.NewCounterClient()) 
				{
					var total = await client.Commands.GetOverallTotalAsync("FooGroup","FooCounter");
					total.Should().Be(1);
				}
			}
		}

		[Theory]
		[InlineData(15, 322)]
		[InlineData(251, 252)]
		public async Task Using_multiple_different_batches_for_the_same_store_async_should_work(int totalForT1, int totalForT2)
		{
			using (var store = NewRemoteCountersStore(createDefaultCounter:true))
			{
				var t1 = Task.Run(() =>
				{
					using (var batch = store.Advanced.NewBatch(store.DefaultCounterStorageName,new CountersBatchOptions
					{
						BatchSizeLimit = totalForT1 / 3
					}))
					{
						for (int i = 0; i < totalForT1; i++)
							batch.ScheduleIncrement("G", "C");
					}
				});

				var t2 = Task.Run(() =>
				{
					using (var batch = store.Advanced.NewBatch(store.DefaultCounterStorageName, new CountersBatchOptions
					{
						BatchSizeLimit = totalForT2 / 3
					}))
					{
						for (int i = 0; i < totalForT2; i++)
							batch.ScheduleIncrement("G", "C");
					}
				});

				await Task.WhenAll(t1, t2);

				using (var client = store.NewCounterClient())
				{
					var total = await client.Commands.GetOverallTotalAsync("G", "C");
					total.Should().Be(totalForT1 + totalForT2);
				}

			}
		}

		[Fact]
		public async Task Using_multiple_different_batches_for_different_stores_async_should_work()
		{
			using (var store = NewRemoteCountersStore())
			{
				await store.CreateCounterStorageAsync(CreateCounterStorageDocument(CounterStorageName1), CounterStorageName1);
				await store.CreateCounterStorageAsync(CreateCounterStorageDocument(CounterStorageName2), CounterStorageName2);

				var t1 = Task.Run(() =>
				{
					for (int i = 0; i < 499; i++)
						store.Batch[CounterStorageName1].ScheduleIncrement("G", "C");
					store.Batch[CounterStorageName1].FlushAsync().Wait();
				});

				var t2 = Task.Run(() =>
				{
					for (int i = 0; i < 500; i++)
						store.Batch[CounterStorageName2].ScheduleIncrement("G", "C");
					store.Batch[CounterStorageName2].FlushAsync().Wait();
				});

				await Task.WhenAll(t1, t2);

				using (var client = store.NewCounterClient(CounterStorageName1))
				{
					var total = await client.Commands.GetOverallTotalAsync("G", "C");
					total.Should().Be(499);
				}

				using (var client = store.NewCounterClient(CounterStorageName2))
				{
					var total = await client.Commands.GetOverallTotalAsync("G", "C");
					total.Should().Be(500);
				}
			}
		}

		[Fact]
		public async Task Using_batch_multithreaded_should_work()
		{
			using (var store = NewRemoteCountersStore())
			{
				await store.CreateCounterStorageAsync(CreateCounterStorageDocument(CounterStorageName1), CounterStorageName1);

				var t1 = Task.Run(() =>
				{
					for (int i = 0; i < 500; i++)
						store.Batch[CounterStorageName1].ScheduleIncrement("G", "C");
				});

				var t2 = Task.Run(() =>
				{
					for (int i = 0; i < 500; i++)
						store.Batch[CounterStorageName1].ScheduleIncrement("G", "C");
				});

				await Task.WhenAll(t1, t2);
				await store.Batch[CounterStorageName1].FlushAsync();

				using (var client = store.NewCounterClient(CounterStorageName1))
				{
					var total = await client.Commands.GetOverallTotalAsync("G", "C");
					total.Should().Be(1000);
				}
			}
		}

		[Theory]
		[InlineData(33)]
		[InlineData(100)]
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
