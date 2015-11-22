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
        private const string CounterName2 = "Bar2";

        [Fact]
        public async Task NotificationReceivedWhenCounterAddedAndIncremented()
        {
            using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
            {
                var changes = store.Changes();
                var notificationTask = changes.Task.Result
                    .ForChange(GroupName, CounterName)
                    .Timeout(TimeSpan.FromSeconds(5))
                    .Take(1).ToTask();

                changes.WaitForAllPendingSubscriptions();
                await store.IncrementAsync(GroupName, CounterName);

                var counterChange = await notificationTask;
                Assert.Equal(GroupName, counterChange.GroupName);
                Assert.Equal(CounterName, counterChange.CounterName);
                Assert.Equal(CounterChangeAction.Add, counterChange.Action);
                Assert.Equal(1, counterChange.Total);

                notificationTask = changes.Task.Result
                    .ForChange(GroupName, CounterName)
                    .Timeout(TimeSpan.FromSeconds(5))
                    .Take(1).ToTask();

                changes.WaitForAllPendingSubscriptions();
                await store.ChangeAsync(GroupName, CounterName, 6);

                counterChange = await notificationTask;
                Assert.Equal(GroupName, counterChange.GroupName);
                Assert.Equal(CounterName, counterChange.CounterName);
                Assert.Equal(CounterChangeAction.Increment, counterChange.Action);
                Assert.Equal(7, counterChange.Total);
            }
        }

        [Fact]
        public async Task NotificationReceivedForCountersStartingWith()
        {
            using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
            {
                var changes = store.Changes();
                var notificationTask = changes.Task.Result
                    .ForCountersStartingWith(GroupName, CounterName.Substring(0, 2))
                    .Timeout(TimeSpan.FromSeconds(5))
                    .Take(1).ToTask();

                changes.WaitForAllPendingSubscriptions();
                await store.ChangeAsync(GroupName, CounterName, 2);

                var counterChange = await notificationTask;
                Assert.Equal(GroupName, counterChange.GroupName);
                Assert.Equal(CounterName, counterChange.CounterName);
                Assert.Equal(CounterChangeAction.Add, counterChange.Action);
                Assert.Equal(2, counterChange.Total);

                notificationTask = changes.Task.Result
                 .ForCountersStartingWith(GroupName, CounterName)
                 .Timeout(TimeSpan.FromSeconds(5))
                 .Take(1).ToTask();

                changes.WaitForAllPendingSubscriptions();
                await store.DecrementAsync(GroupName, CounterName2);

                counterChange = await notificationTask;
                Assert.Equal(GroupName, counterChange.GroupName);
                Assert.Equal(CounterName2, counterChange.CounterName);
                Assert.Equal(CounterChangeAction.Add, counterChange.Action);
                Assert.Equal(-1, counterChange.Total);
            }
        }

        [Fact]
        public async Task NotificationReceivedForCountersStartingWith2()
        {
            using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
            {
                var changes = store.Changes();
                var finished = false;
                changes.Task.Result
                    .ForCountersStartingWith(GroupName, CounterName.Substring(0, 2))
                    .Subscribe(counterChange =>
                    {
                        if (finished)
                            return;

                        Assert.Equal(GroupName, counterChange.GroupName);
                        Assert.Equal(CounterName, counterChange.CounterName);
                        Assert.Equal(CounterChangeAction.Add, counterChange.Action);
                        Assert.Equal(2, counterChange.Total);
                        finished = true;
                    });

                changes.WaitForAllPendingSubscriptions();
                await store.ChangeAsync(GroupName, CounterName, 2);

                var notificationTask2 = changes.Task.Result
                 .ForCountersStartingWith(GroupName, CounterName)
                 .Timeout(TimeSpan.FromSeconds(5))
                 .Take(1).ToTask();

                changes.WaitForAllPendingSubscriptions();
                await store.DecrementAsync(GroupName, CounterName2);

                var counterChange2 = await notificationTask2;
                Assert.Equal(GroupName, counterChange2.GroupName);
                Assert.Equal(CounterName2, counterChange2.CounterName);
                Assert.Equal(CounterChangeAction.Add, counterChange2.Action);
                Assert.Equal(-1, counterChange2.Total);
            }
        }

        [Fact]
        public async Task notification_received_when_counter_in_group_added_and_incremented()
        {
            using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
            {
                var changes = store.Changes();
                var notificationTask = changes.Task.Result
                    .ForCountersInGroup(GroupName)
                    .Timeout(TimeSpan.FromSeconds(5))
                    .Take(1).ToTask();

                changes.WaitForAllPendingSubscriptions();
                await store.ChangeAsync(GroupName, CounterName, 2);

                var counterChange = await notificationTask;
                Assert.Equal(GroupName, counterChange.GroupName);
                Assert.Equal(CounterName, counterChange.CounterName);
                Assert.Equal(CounterChangeAction.Add, counterChange.Action);
                Assert.Equal(2, counterChange.Total);

                notificationTask = changes.Task.Result
                    .ForCountersInGroup(GroupName)
                    .Timeout(TimeSpan.FromSeconds(5))
                    .Take(1).ToTask();

                changes.WaitForAllPendingSubscriptions();
                await store.DecrementAsync(GroupName + GroupName, CounterName2);
                await store.DecrementAsync(GroupName, CounterName2);

                counterChange = await notificationTask;
                Assert.Equal(GroupName, counterChange.GroupName);
                Assert.Equal(CounterName2, counterChange.CounterName);
                Assert.Equal(CounterChangeAction.Add, counterChange.Action);
                Assert.Equal(-1, counterChange.Total);
            }
        }

        [Fact]
        public async Task notification_received_when_replication_counter_added_and_incremented()
        {
            using (var storeA = NewRemoteCountersStore(DefaultCounterStorageName + "A"))
            using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName + "B"))
            {
                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                var changesB = storeB.Changes();
                var notificationTask = changesB.Task.Result
                    .ForChange(GroupName, CounterName)
                    .Timeout(TimeSpan.FromSeconds(10))
                    .Take(1).ToTask();

                changesB.WaitForAllPendingSubscriptions();

                await storeA.IncrementAsync(GroupName, CounterName);

                var counterChange = await notificationTask;
                Assert.Equal(GroupName, counterChange.GroupName);
                Assert.Equal(CounterName, counterChange.CounterName);
                Assert.Equal(CounterChangeAction.Add, counterChange.Action);

                //now connecting to changes in storeA
                var changesA = storeA.Changes();
                notificationTask = changesA.Task.Result
                    .ForChange(GroupName, CounterName)
                    .Timeout(TimeSpan.FromSeconds(10))
                    .Take(1).ToTask();

                changesA.WaitForAllPendingSubscriptions();

                await storeB.IncrementAsync(GroupName, CounterName);

                counterChange = await notificationTask;
                Assert.Equal(GroupName, counterChange.GroupName);
                Assert.Equal(CounterName, counterChange.CounterName);
                Assert.Equal(CounterChangeAction.Increment, counterChange.Action);
            }
        }

        [Fact]
        public async Task notification_received_when_replication_counter_subscribed_to_group()
        {
            using (var storeA = NewRemoteCountersStore(DefaultCounterStorageName + "A"))
            using (var storeB = NewRemoteCountersStore(DefaultCounterStorageName + "B"))
            {
                await SetupReplicationAsync(storeA, storeB);
                await SetupReplicationAsync(storeB, storeA);

                var changesB = storeB.Changes();
                var notificationTask = changesB.Task.Result
                    .ForCountersInGroup(GroupName)
                    .Timeout(TimeSpan.FromSeconds(10))
                    .Take(1).ToTask();
                await storeA.IncrementAsync(GroupName, CounterName);

                changesB.WaitForAllPendingSubscriptions();

                var counterChange = await notificationTask;
                Assert.Equal(GroupName, counterChange.GroupName);
                Assert.Equal(CounterName, counterChange.CounterName);
                Assert.Equal(CounterChangeAction.Add, counterChange.Action);

                //now connecting to changes in storeA
                var changesA = storeA.Changes();
                notificationTask = changesA.Task.Result
                    .ForCountersInGroup(GroupName)
                    .Timeout(TimeSpan.FromSeconds(10))
                    .Take(1).ToTask();

                changesA.WaitForAllPendingSubscriptions();

                await storeB.IncrementAsync(GroupName, CounterName);

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
            using (var store = NewRemoteCountersStore(DefaultCounterStorageName))
            {
                using (var batchOperation = store.Advanced.NewBatch(new CountersBatchOptions { BatchSizeLimit = batchSizeLimit }))
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

                var ratio = (int)Math.Ceiling((double)actionsCount / batchSizeLimit);
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