using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Client.Extensions;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Counters
{
	public class CountersBatchTests : RavenBaseCountersTest
	{
		private const string OtherCounterStorageName = "OtherFooBarCounter";

		[Fact]
		public async Task CountersBatch_increment_decrement_should_work()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				using (var otherStore = await store.Admin.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\Cs1"}
					},
				}, OtherCounterStorageName))
				{
					otherStore.Initialize();
					using (var batchOperation = otherStore.Advanced.NewBatch(new CountersBatchOptions { BatchSizeLimit = 3 }))
					{
						batchOperation.ScheduleIncrement("FooGroup", "FooCounter");
						batchOperation.ScheduleIncrement("FooGroup", "FooCounter");
						batchOperation.ScheduleDecrement("FooGroup", "FooCounter");
					}
					var total = await otherStore.GetOverallTotalAsync("FooGroup", "FooCounter");
					Assert.Equal(1, total);
				}
			}
		}

		[Fact]
		public async Task CountersBatch_increment_decrement_with_different_counters_in_the_batch_should_work()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				using (var batchOperation = store.Advanced.NewBatch(new CountersBatchOptions { BatchSizeLimit = 3 }))
				{
					batchOperation.ScheduleIncrement("FooGroup", "FooCounter");
					batchOperation.ScheduleIncrement("FooGroup", "FooCounter");
					batchOperation.ScheduleDecrement("FooGroup", "FooCounter");

					batchOperation.ScheduleIncrement("FooGroup", "FooCounter2");
					batchOperation.ScheduleDecrement("FooGroup", "FooCounter2");
					batchOperation.ScheduleDecrement("FooGroup", "FooCounter2");
				}

				{
					var total = await store.GetOverallTotalAsync("FooGroup", "FooCounter2");
                    Assert.Equal(-1, total);

                    total = await store.GetOverallTotalAsync("FooGroup", "FooCounter");
                    Assert.Equal(1, total);
                }
			}
		}


		[Theory]
		[InlineData(33)]
		[InlineData(50)]
		public async Task CountersBatch_with_multiple_batches_should_work(int countOfOperationsInBatch)
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				using (var counterBatch = store.Advanced.NewBatch(new CountersBatchOptions {BatchSizeLimit = countOfOperationsInBatch/2}))
				{
					int x = 0;
					for (int i = 0; i < countOfOperationsInBatch; i++)
					{
						counterBatch.ScheduleIncrement("FooGroup", "FooCounter");
						x++;
					}
				}
				var total = await store.GetOverallTotalAsync("FooGroup", "FooCounter");
                Assert.Equal(countOfOperationsInBatch, total);
            }
		}

		[Fact]
		public async Task CounterBatch_using_batch_store_for_default_store_should_work()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				store.Batch.ScheduleIncrement("FooGroup", "FooCounter"); //schedule increment for default counter storage
				await store.Batch.FlushAsync();

				//with counter storage name null - client is opened for default counter

				var total = await store.GetOverallTotalAsync("FooGroup", "FooCounter");
                Assert.Equal(1, total);
            }
		}

		[Theory]
		[InlineData(15, 322)]
		[InlineData(251, 252)]
		public async Task Using_multiple_different_batches_for_the_same_store_async_should_work(int totalForT1, int totalForT2)
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				var t1 = Task.Run(() =>
				{
					using (var batch = store.Advanced.NewBatch(new CountersBatchOptions
					{
						BatchSizeLimit = totalForT1/3
					}))
					{
						for (int i = 0; i < totalForT1; i++)
							batch.ScheduleIncrement("G", "C");
					}
				});

				var t2 = Task.Run(() =>
				{
					using (var batch = store.Advanced.NewBatch(new CountersBatchOptions
					{
						BatchSizeLimit = totalForT2/3
					}))
					{
						for (int i = 0; i < totalForT2; i++)
							batch.ScheduleIncrement("G", "C");
					}
				});

				await Task.WhenAll(t1, t2);
				
				var total = await store.GetOverallTotalAsync("G", "C");
                Assert.Equal(totalForT1 + totalForT2, total);
            }
		}

		[Fact]
		public async Task Using_multiple_different_batches_for_different_stores_async_should_work()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				using (var otherStore = await store.Admin.CreateCounterStorageAsync(MultiDatabase.CreateCounterStorageDocument(OtherCounterStorageName), OtherCounterStorageName))
				{
					otherStore.Initialize();

					var t1 = Task.Run(() =>
					{
						for (int i = 0; i < 499; i++)
							store.Batch[OtherCounterStorageName].ScheduleIncrement("G", "C");
						store.Batch[OtherCounterStorageName].FlushAsync().Wait();
					});

					var t2 = Task.Run(() =>
					{
						for (int i = 0; i < 500; i++)
							store.Batch[store.Name].ScheduleIncrement("G", "C");
						store.Batch[store.Name].FlushAsync().Wait();
					});

					await Task.WhenAll(t1, t2);

					var total = await otherStore.GetOverallTotalAsync("G", "C");
					
                    Assert.Equal(499, total);

                    total = await store.GetOverallTotalAsync("G", "C");
                    Assert.Equal(500, total);

                }
            }
		}

		[Fact]
		public async Task Using_batch_multithreaded_should_work()
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				using (var otherStore = await store.Admin.CreateCounterStorageAsync(MultiDatabase.CreateCounterStorageDocument(OtherCounterStorageName), OtherCounterStorageName))
				{
					otherStore.Initialize();

					var t1 = Task.Run(() =>
					{
						for (int i = 0; i < 500; i++)
							store.Batch[OtherCounterStorageName].ScheduleIncrement("G", "C");
					});

					var t2 = Task.Run(() =>
					{
						for (int i = 0; i < 500; i++)
							store.Batch[OtherCounterStorageName].ScheduleIncrement("G", "C");
					});

					await Task.WhenAll(t1, t2);
					await store.Batch[OtherCounterStorageName].FlushAsync();

					var total = await otherStore.GetOverallTotalAsync("G", "C");
					
                    Assert.Equal(1000, total);

                }
            }
		}

		[Theory]
		[InlineData(33)]
		[InlineData(100)]
		public async Task CountersBatch_using_batch_store_should_work(int countOfOperationsInBatch)
		{
			using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
			{
				using (var otherStore = await store.Admin.CreateCounterStorageAsync(new CounterStorageDocument
				{
					Settings = new Dictionary<string, string>
					{
						{"Raven/Counters/DataDir", @"~\Counters\" + OtherCounterStorageName}
					},
				}, OtherCounterStorageName))
				{
					otherStore.Initialize();

					store.Batch[OtherCounterStorageName].DefaultOptions.BatchSizeLimit = countOfOperationsInBatch/2;
					int x = 0;
					for (int i = 0; i < countOfOperationsInBatch; i++)
					{
						store.Batch[OtherCounterStorageName].ScheduleIncrement("FooGroup", "FooCounter");
						x++;
					}

					await store.Batch[OtherCounterStorageName].FlushAsync();
					var total = await otherStore.GetOverallTotalAsync("FooGroup", "FooCounter");
                    Assert.Equal(countOfOperationsInBatch, total);

                }
            }
		}
	}
}
