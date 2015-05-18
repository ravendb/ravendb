using System;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Counters.Notifications;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Counters
{
	public class CountersChangesTests : RavenBaseCountersTest
	{
		private const string GroupName = "Foo";
		private const string CounterName = "Bar";

		[Fact]
		public async Task NotificationReceivedWhenCounterAddedAndIncremented()
		{
			using (var store = NewRemoteCountersStore())
			{
				var changes = store.Changes();
				var notificationTask = changes.Task.Result
					.ForChange(GroupName, CounterName)
					.Timeout(TimeSpan.FromSeconds(2))
					.Take(1).ToTask();

				changes.WaitForAllPendingSubscriptions();

				using (var client = store.NewCounterClient())
				{
					await client.Commands.IncrementAsync(GroupName, CounterName);
				}

				var counterChange = await notificationTask;
				Assert.Equal(GroupName, counterChange.GroupName);
				Assert.Equal(CounterName, counterChange.CounterName);
				Assert.Equal(CounterChangeAction.Add, counterChange.Action);

				notificationTask = changes.Task.Result
					.ForChange(GroupName, CounterName)
					.Timeout(TimeSpan.FromSeconds(2))
					.Take(1).ToTask();

				changes.WaitForAllPendingSubscriptions();

				using (var client = store.NewCounterClient())
				{
					await client.Commands.IncrementAsync(GroupName, CounterName);
				}

				counterChange = await notificationTask;
				Assert.Equal(GroupName, counterChange.GroupName);
				Assert.Equal(CounterName, counterChange.CounterName);
				Assert.Equal(CounterChangeAction.Increment, counterChange.Action);
			}
		}

		[Fact]
		public async Task NotificationReceivedWhenLocalCounterAddedAndDecremented()
		{
			using (var store = NewRemoteCountersStore())
			{
				var changes = store.Changes();
				var notificationTask = changes.Task.Result
					.ForLocalCounterChange(GroupName, CounterName)
					.Timeout(TimeSpan.FromSeconds(2))
					.Take(1).ToTask();

				changes.WaitForAllPendingSubscriptions();

				using (var client = store.NewCounterClient())
				{
					await client.Commands.IncrementAsync(GroupName, CounterName);
				}

				var counterChange = await notificationTask;
				Assert.Equal(GroupName, counterChange.GroupName);
				Assert.Equal(CounterName, counterChange.CounterName);
				Assert.Equal(CounterChangeAction.Add, counterChange.Action);

				notificationTask = changes.Task.Result
					.ForLocalCounterChange(GroupName, CounterName)
					.Timeout(TimeSpan.FromSeconds(2))
					.Take(1).ToTask();

				changes.WaitForAllPendingSubscriptions();

				using (var client = store.NewCounterClient())
				{
					await client.Commands.DecrementAsync(GroupName, CounterName);
				}

				counterChange = await notificationTask;
				Assert.Equal(GroupName, counterChange.GroupName);
				Assert.Equal(CounterName, counterChange.CounterName);
				Assert.Equal(CounterChangeAction.Decrement, counterChange.Action);
			}
		}

		[Fact]
		public async Task NotificationReceivedWhenReplicationCounterAddedAndIncremented()
		{
			using (var storeA = NewRemoteCountersStore(DefaultCounteStorageName + "A"))
			using (var storeB = NewRemoteCountersStore(DefaultCounteStorageName + "B"))
			{
				await SetupReplicationAsync(storeA, storeB);
				await SetupReplicationAsync(storeB, storeA);

				var changesB = storeB.Changes();
				var notificationTask = changesB.Task.Result
					.ForReplicationChange(GroupName, CounterName)
					.Timeout(TimeSpan.FromSeconds(30))
					.Take(1).ToTask();

				using (var client = storeA.NewCounterClient())
				{
					await client.Commands.IncrementAsync(GroupName, CounterName);
				}

				changesB.WaitForAllPendingSubscriptions();

				var counterChange = await notificationTask;
				Assert.Equal(GroupName, counterChange.GroupName);
				Assert.Equal(CounterName, counterChange.CounterName);
				Assert.Equal(CounterChangeAction.Add, counterChange.Action);

				//now connecting to changes in storeA
				var changesA = storeA.Changes();
				notificationTask = changesA.Task.Result
					.ForReplicationChange(GroupName, CounterName)
					.Timeout(TimeSpan.FromSeconds(30))
					.Take(1).ToTask();

				changesA.WaitForAllPendingSubscriptions();

				using (var client = storeB.NewCounterClient())
				{
					await client.Commands.IncrementAsync(GroupName, CounterName);
				}

				counterChange = await notificationTask;
				Assert.Equal(GroupName, counterChange.GroupName);
				Assert.Equal(CounterName, counterChange.CounterName);
				Assert.Equal(CounterChangeAction.Increment, counterChange.Action);
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
			using (var store = NewRemoteCountersStore())
			{
				using (var batchOperation = store.Advanced.NewBatch(store.DefaultCounterStorageName,
														new CountersBatchOptions { BatchSizeLimit = batchSizeLimit }))
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
							batchOperation.ScheduleIncrement("FooGroup", "FooCounter");
						else
							batchOperation.ScheduleDecrement("FooGroup", "FooCounter");
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
