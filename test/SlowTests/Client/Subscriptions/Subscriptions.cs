using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client;
using FastTests.Client.Subscriptions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class Subscriptions : SubscriptionTestBase
    {
        public Subscriptions(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CreateSubscription()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionCreationParams = new SubscriptionCreationOptions
                {
                    Query = "from People"
                };
                var subsName = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                var subscriptionsConfig = await store.Subscriptions.GetSubscriptionsAsync(0, 10);
                var subscripitonState = await store.Subscriptions.GetSubscriptionStateAsync(subsName);

                Assert.Equal(1, subscriptionsConfig.Count);
                Assert.Equal(subscriptionCreationParams.Query, subscriptionsConfig[0].Query);
                Assert.Null(subscriptionsConfig[0].ChangeVectorForNextBatchStartingPoint);
                Assert.Equal(subsName, subscriptionsConfig[0].SubscriptionName);
                Assert.Equal(subscripitonState.SubscriptionId, subscriptionsConfig[0].SubscriptionId);
            }
        }

        [Fact]
        public async Task BasicSusbscriptionTest()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastChangeVector = (await store.Maintenance.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector;
                await CreateDocuments(store, 5);

                var subscriptionCreationParams = new SubscriptionCreationOptions()
                {
                    Query = "from Things",
                    ChangeVector = lastChangeVector
                };
                var subsId = await store.Subscriptions.CreateAsync(subscriptionCreationParams);
                using (var subscription = store.Subscriptions.GetSubscriptionWorker<Thing>(new SubscriptionWorkerOptions(subsId) {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var list = new BlockingCollection<Thing>();
                    GC.KeepAlive(subscription.Run(u =>
                    {
                        foreach (var item in u.Items)
                        {
                            list.Add(item.Result);
                        }
                    }));
                    Thing thing;
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(list.TryTake(out thing, 1000));
                    }
                    Assert.False(list.TryTake(out thing, 50));
                }
            }
        }

        [Fact]
        public async Task SubscriptionStrategyConnectIfFree()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastChangeVector = (await store.Maintenance.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector ?? null;
                await CreateDocuments(store, 5);

                var subscriptionCreationParams = new SubscriptionCreationOptions()
                {
                    Query = "from Things",
                    ChangeVector = lastChangeVector
                };
                var subsId = await store.Subscriptions.CreateAsync(subscriptionCreationParams);
                using (
                    var acceptedSubscription = store.Subscriptions.GetSubscriptionWorker<Thing>(new SubscriptionWorkerOptions(subsId)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                {
                    var acceptedSubscriptionList = new BlockingCollection<Thing>();

                    var firstSubscriptionTask = acceptedSubscription.Run(u =>
                    {
                        foreach (var item in u.Items)
                        {
                            acceptedSubscriptionList.Add(item.Result);
                        }
                    });
                    GC.KeepAlive(firstSubscriptionTask);


                    Thing thing;

                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(acceptedSubscriptionList.TryTake(out thing, 1000));
                    }

                    Assert.False(acceptedSubscriptionList.TryTake(out thing, 50));
                    // open second subscription
                    using (var rejectedSubscription =
                        store.Subscriptions.GetSubscriptionWorker<Thing>(new SubscriptionWorkerOptions(subsId)
                        {
                            Strategy = SubscriptionOpeningStrategy.OpenIfFree,
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(2000)
                        }))
                    {
                        // sometime not throwing (on linux) when written like this:
                        // await Assert.ThrowsAsync<SubscriptionInUseException>(async () => await rejectedSubscription.StartAsync());
                        // so we put this in a try block
                        try
                        {
                            await rejectedSubscription.Run(_ => { });
                            Assert.False(true, "Exepcted a throw here");
                        }
                        catch (SubscriptionInUseException)
                        {
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task SubscriptionWaitStrategy()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastChangeVector = (await store.Maintenance.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector;

                var subscriptionCreationParams = new SubscriptionCreationOptions()
                {
                    Query = "from Things",
                    ChangeVector = lastChangeVector
                };

                await CreateDocuments(store, 5);
                var subsId = await store.Subscriptions.CreateAsync(subscriptionCreationParams);
                using (
                    var acceptedSubscription = store.Subscriptions.GetSubscriptionWorker<Thing>(new SubscriptionWorkerOptions(subsId) {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                {

                    var acceptedSusbscriptionList = new BlockingCollection<Thing>();
                    var waitingSubscriptionList = new BlockingCollection<Thing>();

                    var ackSentAmre = new AsyncManualResetEvent();
                    acceptedSubscription.AfterAcknowledgment += b => { ackSentAmre.Set(); return Task.CompletedTask; };


                    GC.KeepAlive(acceptedSubscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            acceptedSusbscriptionList.Add(item.Result);
                        }
                        Thread.Sleep(20);
                    }));

                    // wait until we know that connection was established

                    Thing thing;
                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(acceptedSusbscriptionList.TryTake(out thing, 50000));
                    }

                    Assert.False(acceptedSusbscriptionList.TryTake(out thing, 50));

                    // open second subscription
                    using (
                        var waitingSubscription =
                            store.Subscriptions.GetSubscriptionWorker<Thing>(new SubscriptionWorkerOptions(subsId)
                            {
                                Strategy = SubscriptionOpeningStrategy.WaitForFree,
                                TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(250)
                            }))
                    {

                        GC.KeepAlive(waitingSubscription.Run(x =>
                        {
                            foreach (var item in x.Items)
                            {
                                waitingSubscriptionList.Add(item.Result);
                            }
                        }));

                        Assert.True(await ackSentAmre.WaitAsync(TimeSpan.FromSeconds(50)));

                        acceptedSubscription.Dispose();

                        await CreateDocuments(store, 5);

                        // wait until we know that connection was established
                        for (var i = 0; i < 5; i++)
                        {
                            Assert.True(waitingSubscriptionList.TryTake(out thing, 3000));
                        }

                        Assert.False(waitingSubscriptionList.TryTake(out thing, 50));
                    }
                }
            }
        }

        [Fact]
        public async Task SubscriptionSimpleTakeOverStrategy()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastChangeVector = (await store.Maintenance.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector ?? null;
                await CreateDocuments(store, 5);

                var subscriptionCreationParams = new SubscriptionCreationOptions()
                {
                    Query = "from Things",
                    ChangeVector = lastChangeVector
                };

                var subsId = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                using (
                    var acceptedSubscription = store.Subscriptions.GetSubscriptionWorker<Thing>(new SubscriptionWorkerOptions(subsId) {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                {
                    var acceptedSusbscriptionList = new BlockingCollection<Thing>();
                    var takingOverSubscriptionList = new BlockingCollection<Thing>();
                    long counter = 0;

                    var batchProccessedByFirstSubscription = new AsyncManualResetEvent();

                    acceptedSubscription.AfterAcknowledgment +=
                        b =>
                        {
                            if (Interlocked.Read(ref counter) == 5)
                                batchProccessedByFirstSubscription.Set();
                            return Task.CompletedTask;
                        };

                    var firstRun = acceptedSubscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            Interlocked.Increment(ref counter);
                            acceptedSusbscriptionList.Add(item.Result);
                        }
                    });


                    Thing thing;

                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(acceptedSusbscriptionList.TryTake(out thing, 5000), "no doc");
                    }

                    Assert.True(await batchProccessedByFirstSubscription.WaitAsync(TimeSpan.FromSeconds(15)), "no ack");

                    Assert.False(acceptedSusbscriptionList.TryTake(out thing));

                    // open second subscription
                    using (var takingOverSubscription = store.Subscriptions.GetSubscriptionWorker<Thing>(
                        new SubscriptionWorkerOptions(subsId)
                        {
                            TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                            Strategy = SubscriptionOpeningStrategy.TakeOver
                        }))
                    {
                        GC.KeepAlive(takingOverSubscription.Run(x =>
                        {
                            foreach (var item in x.Items)
                            {
                                takingOverSubscriptionList.Add(item.Result);
                            }
                        }));

                        Assert.ThrowsAsync<SubscriptionInUseException>(() => firstRun).Wait();


                        await CreateDocuments(store, 5);

                        // wait until we know that connection was established
                        for (var i = 0; i < 5; i++)
                        {
                            Assert.True(takingOverSubscriptionList.TryTake(out thing, 5000), "no doc takeover");
                        }
                        Assert.False(takingOverSubscriptionList.TryTake(out thing));
                    }
                }
            }
        }

        [Fact]
        public void CanDisableSubscription()
        {
            using (var store = GetDocumentStore())
            {
                string s = store.Subscriptions.Create<Query.Order>();
                var ongoingTask = (OngoingTaskSubscription)store.Maintenance.Send(new GetOngoingTaskInfoOperation(s, OngoingTaskType.Subscription));
                Assert.False(ongoingTask.Disabled);
                store.Maintenance.Send(new ToggleOngoingTaskStateOperation(ongoingTask.TaskId, OngoingTaskType.Subscription, true));
                ongoingTask = (OngoingTaskSubscription)store.Maintenance.Send(new GetOngoingTaskInfoOperation(s, OngoingTaskType.Subscription));
                Assert.True(ongoingTask.Disabled);
            }
        }
        
        private class Command
        {
            public string Error { get; set; }
        }
        
        [Fact]
        public async Task Subscription_WhenFilteredByNull_ShouldWork()
        {
            const string id = "A1";
            
            using var store = GetDocumentStore();
            var subscriptionCreationParams = new SubscriptionCreationOptions
            {
                Query = "from Commands where Error = null"
            };
            var subsName = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

            await using (var acceptedSubscription = store.Subscriptions.GetSubscriptionWorker<Thing>(new SubscriptionWorkerOptions(subsName)))
            {
                var isProcessed = new AsyncManualResetEvent();

                var task = acceptedSubscription.Run(x =>
                {
                    foreach (var item in x.Items)
                    {
                        if(item.Id == id)
                            isProcessed.Set();
                    }
                });

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Command(), id);
                    await session.SaveChangesAsync();
                }

                Assert.True(await isProcessed.WaitAsync(TimeSpan.FromSeconds(15)));
            }
        }
    }
}
