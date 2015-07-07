/*
using System.Threading.Tasks;
using FluentAssertions;
using Raven.Abstractions.TimeSeries;
using Raven.Client.Extensions;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesBatchTests : RavenBaseTimeSeriesTest
	{
		private const string OtherTimeSeriesName = "OtherFooBarTimeSeries";

		[Fact]
		public async Task TimeSeriesBatch_increment_decrement_should_work()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				using (var batchOperation = store.Advanced.NewBatch(new TimeSeriesBatchOptions { BatchSizeLimit = 3 }))
				{
					batchOperation.ScheduleIncrement("FooGroup", "FooTimeSeries");
					batchOperation.ScheduleIncrement("FooGroup", "FooTimeSeries");
					batchOperation.ScheduleDecrement("FooGroup", "FooTimeSeries");
				}
				var total = await store.GetOverallTotalAsync("FooGroup", "FooTimeSeries");
				total.Should().Be(1);
			}
		}

		[Fact]
		public async Task TimeSeriesBatch_increment_decrement_with_different_time_series_in_the_batch_should_work()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				using (var batchOperation = store.Advanced.NewBatch(new TimeSeriesBatchOptions { BatchSizeLimit = 3 }))
				{
					batchOperation.ScheduleIncrement("FooGroup", "FooTimeSeries");
					batchOperation.ScheduleIncrement("FooGroup", "FooTimeSeries");
					batchOperation.ScheduleDecrement("FooGroup", "FooTimeSeries");

					batchOperation.ScheduleIncrement("FooGroup", "FooTimeSeries2");
					batchOperation.ScheduleDecrement("FooGroup", "FooTimeSeries2");
					batchOperation.ScheduleDecrement("FooGroup", "FooTimeSeries2");
				}

				{
					var total = await store.GetOverallTotalAsync("FooGroup", "FooTimeSeries2");
					total.Should().Be(-1);

					total = await store.GetOverallTotalAsync("FooGroup", "FooTimeSeries");
					total.Should().Be(1);
				}
			}
		}


		[Theory]
		[InlineData(33)]
		[InlineData(50)]
		public async Task TimeSeriesBatch_with_multiple_batches_should_work(int countOfOperationsInBatch)
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				using (var timeSeriesBatch = store.Advanced.NewBatch(new TimeSeriesBatchOptions {BatchSizeLimit = countOfOperationsInBatch/2}))
				{
					int x = 0;
					for (int i = 0; i < countOfOperationsInBatch; i++)
					{
						timeSeriesBatch.ScheduleIncrement("FooGroup", "FooTimeSeries");
						x++;
					}
				}
				var total = await store.GetOverallTotalAsync("FooGroup", "FooTimeSeries");
				total.Should().Be(countOfOperationsInBatch);
			}
		}

		[Fact]
		public async Task TimeSeriesBatch_using_batch_store_for_default_store_should_work()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				store.Batch.ScheduleAppend("FooGroup", "FooTimeSeries"); //schedule increment for default timeSeries
				await store.Batch.FlushAsync();

				//with time series name null - client is opened for default timeSeries

				var total = await store.GetOverallTotalAsync("FooGroup", "FooTimeSeries");
				total.Should().Be(1);
			}
		}

		[Theory]
		[InlineData(15, 322)]
		[InlineData(251, 252)]
		public async Task Using_multiple_different_batches_for_the_same_store_async_should_work(int totalForT1, int totalForT2)
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				var t1 = Task.Run(() =>
				{
					using (var batch = store.Advanced.NewBatch(new TimeSeriesBatchOptions
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
					using (var batch = store.Advanced.NewBatch(new TimeSeriesBatchOptions
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
				total.Should().Be(totalForT1 + totalForT2);
			}
		}

		[Fact]
		public async Task Using_multiple_different_batches_for_different_stores_async_should_work()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				using (var otherStore = await store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(OtherTimeSeriesName)))
				{
					otherStore.Initialize();

					var t1 = Task.Run(() =>
					{
						for (int i = 0; i < 499; i++)
							store.Batch[OtherTimeSeriesName].ScheduleIncrement("G", "C");
						store.Batch[OtherTimeSeriesName].FlushAsync().Wait();
					});

					var t2 = Task.Run(() =>
					{
						for (int i = 0; i < 500; i++)
							store.Batch[store.Name].ScheduleIncrement("G", "C");
						store.Batch[store.Name].FlushAsync().Wait();
					});

					await Task.WhenAll(t1, t2);

					var total = await otherStore.GetOverallTotalAsync("G", "C");
					total.Should().Be(499);

					total = await store.GetOverallTotalAsync("G", "C");
					total.Should().Be(500);
				}
			}
		}

		[Fact]
		public async Task Using_batch_multithreaded_should_work()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				using (var otherStore = await store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(OtherTimeSeriesName)))
				{
					otherStore.Initialize();

					var t1 = Task.Run(() =>
					{
						for (int i = 0; i < 500; i++)
							store.Batch[OtherTimeSeriesName].ScheduleIncrement("G", "C");
					});

					var t2 = Task.Run(() =>
					{
						for (int i = 0; i < 500; i++)
							store.Batch[OtherTimeSeriesName].ScheduleIncrement("G", "C");
					});

					await Task.WhenAll(t1, t2);
					await store.Batch[OtherTimeSeriesName].FlushAsync();

					var total = await otherStore.GetOverallTotalAsync("G", "C");
					total.Should().Be(1000);
				}
			}
		}

		[Theory]
		[InlineData(33)]
		[InlineData(100)]
		public async Task TimeSeriesBatch_using_batch_store_should_work(int countOfOperationsInBatch)
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				using (var otherStore = await store.Admin.CreateTimeSeriesAsync(MultiDatabase.CreateTimeSeriesDocument(OtherTimeSeriesName)))
				{
					otherStore.Initialize();

					store.Batch[OtherTimeSeriesName].DefaultOptions.BatchSizeLimit = countOfOperationsInBatch/2;
					int x = 0;
					for (int i = 0; i < countOfOperationsInBatch; i++)
					{
						store.Batch[OtherTimeSeriesName].ScheduleIncrement("FooGroup", "FooTimeSeries");
						x++;
					}

					await store.Batch[OtherTimeSeriesName].FlushAsync();
					var total = await otherStore.GetOverallTotalAsync("FooGroup", "FooTimeSeries");
					total.Should().Be(countOfOperationsInBatch);
				}
			}
		}
	}
}
*/
