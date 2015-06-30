using System;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.TimeSeries;
using Raven.Abstractions.TimeSeries.Notifications;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.TimeSeries
{
	public class TimeSeriesChangesTests : RavenBaseTimeSeriesTest
	{
		[Fact]
		public async Task NotificationReceivedWhenTimeSeriesAppendedAndDeleted()
		{
			using (var store = NewRemoteTimeSeriesStore())
			{
				var changes = store.Changes();
				var notificationTask = changes.Task.Result
					.ForKey("Time")
					.Timeout(TimeSpan.FromSeconds(300))
					.Take(1)
					.ToTask();

				changes.WaitForAllPendingSubscriptions();
				var at = DateTime.Now;
				await store.AppendAsync("Type1", "Time", at, 3d);

				var timeSeriesChange = await notificationTask;
				Assert.Equal("Time", timeSeriesChange.Key);
				Assert.Equal(at.Ticks, timeSeriesChange.At);
				Assert.Equal(TimeSeriesChangeAction.Append, timeSeriesChange.Action);
				Assert.Equal(3d, timeSeriesChange.Values.Single());

				notificationTask = changes.Task.Result
					.ForKey("Time")
					.Timeout(TimeSpan.FromSeconds(300))
					.Take(1)
					.ToTask();

				changes.WaitForAllPendingSubscriptions();
				await store.DeleteAsync("Type1", "Time");

				timeSeriesChange = await notificationTask;
				Assert.Equal("Time", timeSeriesChange.Key);
				Assert.Equal(DateTime.MinValue.Ticks, timeSeriesChange.At);
				Assert.Equal(TimeSeriesChangeAction.Delete, timeSeriesChange.Action);
				Assert.Equal(0, timeSeriesChange.Values.Length);
			}
		}

		[Fact]
		public async Task notification_received_when_replication_time_series_added_and_incremented()
		{
			using (var storeA = NewRemoteTimeSeriesStore())
			using (var storeB = NewRemoteTimeSeriesStore())
			{
				await SetupReplicationAsync(storeA, storeB);
				await SetupReplicationAsync(storeB, storeA);

				var changesB = storeB.Changes();
				var notificationTask = changesB.Task.Result
					.ForKey("Time")
					.Timeout(TimeSpan.FromSeconds(10))
					.Take(1).ToTask();

				changesB.WaitForAllPendingSubscriptions();

				var at = DateTime.Now;
				await storeA.AppendAsync("-Simple", "Time", at, 3d);

				var timeSeriesChange = await notificationTask;
				Assert.Equal("Time", timeSeriesChange.Key);
				Assert.Equal(at.Ticks, timeSeriesChange.At);
				Assert.Equal(TimeSeriesChangeAction.Append, timeSeriesChange.Action);

				//now connecting to changes in storeA
				var changesA = storeA.Changes();
				notificationTask = changesA.Task.Result
					.ForKey("Time")
					.Timeout(TimeSpan.FromSeconds(10))
					.Take(1).ToTask();

				changesA.WaitForAllPendingSubscriptions();

				var at2 = DateTime.Now.AddMinutes(6);
				await storeB.AppendAsync("-Simple", "Is", at2, 6d);

				timeSeriesChange = await notificationTask;
				Assert.Equal("Is", timeSeriesChange.Key);
				Assert.Equal(at2.Ticks, timeSeriesChange.At);
				Assert.Equal(TimeSeriesChangeAction.Append, timeSeriesChange.Action);
			}
		}

		[Theory]
		[InlineData(1, 200)]
		[InlineData(3, 3)]
		[InlineData(50, 30)]
		[InlineData(50, 30)]
		[InlineData(50, 130)]
		public void NotificationReceivedWhenBatchOperation(int batchSizeLimit, int actionsCount)
		{
			int startCount = 0, endCount = 0;
			using (var store = NewRemoteTimeSeriesStore())
			{
				using (var batchOperation = store.Advanced.NewBatch(new TimeSeriesBatchOptions { BatchSizeLimit = batchSizeLimit }))
				{
					store.Changes().Task.Result
						.ForBulkOperation(batchOperation.OperationId).Task.Result
						.Subscribe(changes =>
						{
							switch (changes.Type)
							{
								case BatchType.Started:
									startCount++;
									break;
								case BatchType.Ended:
									endCount++;
									break;
							}
						});

					for (var i = 0; i < actionsCount; i++)
					{
						batchOperation.ScheduleAppend("Time", DateTime.Today.AddMinutes(i));
					}
				}

				var ratio = (int)Math.Ceiling((double)actionsCount/batchSizeLimit);
				WaitUntilCount(startCount, ratio);
				WaitUntilCount(endCount, ratio);
			}
		}

		private static void WaitUntilCount(int count, int expected)
		{
			Assert.True(SpinWait.SpinUntil(() => count == expected, 5000));
		}
	}
}