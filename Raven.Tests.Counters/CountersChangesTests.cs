using System;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Counters.Notifications;
using Xunit;

namespace Raven.Tests.Counters
{
	public class CountersChangesTests : RavenBaseCountersTest
	{
		private volatile string output;
		private const string GroupName = "Foo";
		private const string CounterName = "Bar";

		private void WaitUntilOutput(string expected)
		{
			Assert.True(SpinWait.SpinUntil(() => output == expected, 225000));
		}

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

				var changesB = storeB.Changes();
				var notificationTask = changesB.Task.Result
					.ForReplicationChange(GroupName, CounterName)
					.Timeout(TimeSpan.FromSeconds(10))
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
					.Timeout(TimeSpan.FromSeconds(10))
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
	}
}
