using System;
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
		private const string GroupName = "Foo";
		private const string TimeSeriesName = "Bar";
		private const string TimeSeriesName2 = "Bar2";

		[Fact]
		public async Task NotificationReceivedWhenTimeSeriesAddedAndIncremented()
		{
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName))
			{
				var changes = store.Changes();
				var notificationTask = changes.Task.Result
					.ForChange(GroupName, TimeSeriesName)
					.Timeout(TimeSpan.FromSeconds(300))
					.Take(1).ToTask();

				changes.WaitForAllPendingSubscriptions();
				await store.IncrementAsync(GroupName, TimeSeriesName);

				var timeSeriesChange = await notificationTask;
				Assert.Equal(GroupName, timeSeriesChange.GroupName);
				Assert.Equal(TimeSeriesName, timeSeriesChange.TimeSeriesName);
				Assert.Equal(TimeSeriesChangeAction.Add, timeSeriesChange.Action);
				Assert.Equal(1, timeSeriesChange.Total);

				notificationTask = changes.Task.Result
					.ForChange(GroupName, TimeSeriesName)
					.Timeout(TimeSpan.FromSeconds(300))
					.Take(1).ToTask();

				changes.WaitForAllPendingSubscriptions();
				await store.ChangeAsync(GroupName, TimeSeriesName, 6);

				timeSeriesChange = await notificationTask;
				Assert.Equal(GroupName, timeSeriesChange.GroupName);
				Assert.Equal(TimeSeriesName, timeSeriesChange.TimeSeriesName);
				Assert.Equal(TimeSeriesChangeAction.Increment, timeSeriesChange.Action);
				Assert.Equal(7, timeSeriesChange.Total);
			}
		}

		[Fact]
		public async Task NotificationReceivedForTimeSeriesStartingWith()
		{
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName))
			{
				var changes = store.Changes();
				var notificationTask1 = changes.Task.Result
					.ForTimeSeriesStartingWith(GroupName, TimeSeriesName.Substring(0, 2))
					.Timeout(TimeSpan.FromSeconds(300))
					.Take(1).ToTask();

				changes.WaitForAllPendingSubscriptions();
				await store.ChangeAsync(GroupName, TimeSeriesName, 2);

				var timeSeriesChange1= await notificationTask1;
				Assert.Equal(GroupName, timeSeriesChange1.GroupName);
				Assert.Equal(TimeSeriesName, timeSeriesChange1.TimeSeriesName);
				Assert.Equal(TimeSeriesChangeAction.Add, timeSeriesChange1.Action);
				Assert.Equal(2, timeSeriesChange1.Total);

				var notificationTask2 = changes.Task.Result
					.ForTimeSeriesStartingWith(GroupName, TimeSeriesName)
					.Timeout(TimeSpan.FromSeconds(300))
					.Take(1).ToTask();

				changes.WaitForAllPendingSubscriptions();
				await store.DecrementAsync(GroupName, TimeSeriesName2);

				var timeSeriesChange2 = await notificationTask2;
				Assert.Equal(GroupName, timeSeriesChange2.GroupName);
				Assert.Equal(TimeSeriesName2, timeSeriesChange2.TimeSeriesName);
				Assert.Equal(TimeSeriesChangeAction.Add, timeSeriesChange2.Action);
				Assert.Equal(-1, timeSeriesChange2.Total);
			}
		}

		[Fact]
		public async Task notification_received_when_time_series_in_group_added_and_incremented()
		{
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName))
			{
				var changes = store.Changes();
				var notificationTask = changes.Task.Result
					.ForTimeSeriesInGroup(GroupName)
					.Timeout(TimeSpan.FromSeconds(300))
					.Take(1).ToTask();

				changes.WaitForAllPendingSubscriptions();
				await store.ChangeAsync(GroupName, TimeSeriesName, 2);

				var timeSeriesChange = await notificationTask;
				Assert.Equal(GroupName, timeSeriesChange.GroupName);
				Assert.Equal(TimeSeriesName, timeSeriesChange.TimeSeriesName);
				Assert.Equal(TimeSeriesChangeAction.Add, timeSeriesChange.Action);
				Assert.Equal(2, timeSeriesChange.Total);

				notificationTask = changes.Task.Result
					.ForTimeSeriesInGroup(GroupName)
					.Timeout(TimeSpan.FromSeconds(300))
					.Take(1).ToTask();

				changes.WaitForAllPendingSubscriptions();
				await store.DecrementAsync(GroupName + GroupName, TimeSeriesName2);
				await store.DecrementAsync(GroupName, TimeSeriesName2);

				timeSeriesChange = await notificationTask;
				Assert.Equal(GroupName, timeSeriesChange.GroupName);
				Assert.Equal(TimeSeriesName2, timeSeriesChange.TimeSeriesName);
				Assert.Equal(TimeSeriesChangeAction.Add, timeSeriesChange.Action);
				Assert.Equal(-1, timeSeriesChange.Total);
			}
		}

		[Fact]
		public async Task notification_received_when_replication_time_series_added_and_incremented()
		{
			using (var storeA = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "A"))
			using (var storeB = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);
				await SetupReplicationAsync(storeB, storeA);

				var changesB = storeB.Changes();
				var notificationTask = changesB.Task.Result
					.ForChange(GroupName, TimeSeriesName)
					.Timeout(TimeSpan.FromSeconds(10))
					.Take(1).ToTask();

				changesB.WaitForAllPendingSubscriptions();

				await storeA.IncrementAsync(GroupName, TimeSeriesName);

				var timeSeriesChange = await notificationTask;
				Assert.Equal(GroupName, timeSeriesChange.GroupName);
				Assert.Equal(TimeSeriesName, timeSeriesChange.TimeSeriesName);
				Assert.Equal(TimeSeriesChangeAction.Add, timeSeriesChange.Action);

				//now connecting to changes in storeA
				var changesA = storeA.Changes();
				notificationTask = changesA.Task.Result
					.ForChange(GroupName, TimeSeriesName)
					.Timeout(TimeSpan.FromSeconds(10))
					.Take(1).ToTask();

				changesA.WaitForAllPendingSubscriptions();

				await storeB.IncrementAsync(GroupName, TimeSeriesName);

				timeSeriesChange = await notificationTask;
				Assert.Equal(GroupName, timeSeriesChange.GroupName);
				Assert.Equal(TimeSeriesName, timeSeriesChange.TimeSeriesName);
				Assert.Equal(TimeSeriesChangeAction.Increment, timeSeriesChange.Action);
			}
		}

		[Fact]
		public async Task notification_received_when_replication_time_series_subscribed_to_group()
		{
			using (var storeA = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "A"))
			using (var storeB = NewRemoteTimeSeriesStore(DefaultTimeSeriesName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);
				await SetupReplicationAsync(storeB, storeA);

				var changesB = storeB.Changes();
				var notificationTask = changesB.Task.Result
					.ForTimeSeriesInGroup(GroupName)
					.Timeout(TimeSpan.FromSeconds(30))
					.Take(1).ToTask();
				await storeA.IncrementAsync(GroupName, TimeSeriesName);

				changesB.WaitForAllPendingSubscriptions();

				var timeSeriesChange = await notificationTask;
				Assert.Equal(GroupName, timeSeriesChange.GroupName);
				Assert.Equal(TimeSeriesName, timeSeriesChange.TimeSeriesName);
				Assert.Equal(TimeSeriesChangeAction.Add, timeSeriesChange.Action);

				//now connecting to changes in storeA
				var changesA = storeA.Changes();
				notificationTask = changesA.Task.Result
					.ForTimeSeriesInGroup(GroupName)
					.Timeout(TimeSpan.FromSeconds(30))
					.Take(1).ToTask();

				changesA.WaitForAllPendingSubscriptions();

				await storeB.IncrementAsync(GroupName, TimeSeriesName);

				timeSeriesChange = await notificationTask;
				Assert.Equal(GroupName, timeSeriesChange.GroupName);
				Assert.Equal(TimeSeriesName, timeSeriesChange.TimeSeriesName);
				Assert.Equal(TimeSeriesChangeAction.Increment, timeSeriesChange.Action);
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
			using (var store = NewRemoteTimeSeriesStore(DefaultTimeSeriesName))
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
						if (i % 2 == 0)
							batchOperation.ScheduleIncrement("FooGroup", "FooTimeSeries");
						else
							batchOperation.ScheduleDecrement("FooGroup", "FooTimeSeries");
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
