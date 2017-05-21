using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Documents.Notifications;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using Xunit;
using Sparrow;

namespace FastTests.Client.Subscriptions
{
    public class Subscriptions : SubscriptionTestBase
    {
        [Fact]
        public async Task CreateSubscription()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionCreationParams = new SubscriptionCreationOptions
                {
                    Criteria = new SubscriptionCriteria("People")
                };  
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCreationParams);

                var subscriptionsConfig = await store.AsyncSubscriptions.GetSubscriptionsAsync(0, 10);

                Assert.Equal(1, subscriptionsConfig.Count);
                Assert.Equal(subscriptionCreationParams.Criteria.Collection, subscriptionsConfig[0].Criteria.Collection);
                Assert.Equal(subscriptionCreationParams.Criteria.FilterJavaScript, subscriptionsConfig[0].Criteria.FilterJavaScript);
                Assert.Equal(0, subscriptionsConfig[0].ChangeVector.Length);
                Assert.Equal(subsId, subscriptionsConfig[0].SubscriptionId);
            }
        }

        [Fact]
        public async Task BasicSusbscriptionTest()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastChangeVector = (await store.Admin.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector;
                await CreateDocuments(store, 5);

                var subscriptionCreationParams = new SubscriptionCreationOptions()
                {
                    Criteria = new SubscriptionCriteria("Things"),
                    ChangeVector = lastChangeVector
                };
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCreationParams);
                using (var subscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)))
                {
                    var list = new BlockingCollection<Thing>();
                    subscription.Subscribe<Thing>(x =>
                    {
                        list.Add(x);
                    });
                    await subscription.StartAsync();

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

                var lastChangeVector = (await store.Admin.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector ?? null;
                await CreateDocuments(store, 5);

                var subscriptionCreationParams = new SubscriptionCreationOptions()
                {
                    Criteria = new SubscriptionCriteria("Things"),
                    ChangeVector = lastChangeVector
                };
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCreationParams);
                using (
                    var acceptedSubscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)
                    {
                        TimeToWaitBeforeConnectionRetryMilliseconds = 20000
                    }))
                {
                    var acceptedSusbscriptionList = new BlockingCollection<Thing>();
                    acceptedSubscription.Subscribe(x => { acceptedSusbscriptionList.Add(x); });
                    await acceptedSubscription.StartAsync();

                    Thing thing;

                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(acceptedSusbscriptionList.TryTake(out thing, 1000));
                    }

                    Assert.False(acceptedSusbscriptionList.TryTake(out thing, 50));
                    // open second subscription
                    using (var rejectedSusbscription =
                        store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)
                        {
                            Strategy = SubscriptionOpeningStrategy.OpenIfFree,
                            TimeToWaitBeforeConnectionRetryMilliseconds = 2000
                        }))
                    {
                        rejectedSusbscription.Subscribe(thing1 => { });

                        // sometime not throwing (on linux) when written like this:
                        // await Assert.ThrowsAsync<SubscriptionInUseException>(async () => await rejectedSusbscription.StartAsync());
                        // so we put this in a try block
                        try
                        {
                            await rejectedSusbscription.StartAsync();
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

                var lastChangeVector = (await store.Admin.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector;

                var subscriptionCreationParams = new SubscriptionCreationOptions()
                {
                    Criteria = new SubscriptionCriteria("Things"),
                    ChangeVector = lastChangeVector
                };

                await CreateDocuments(store, 5);
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCreationParams);
                using (
                    var acceptedSubscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)))
                {

                    var acceptedSusbscriptionList = new BlockingCollection<Thing>();
                    var waitingSubscriptionList = new BlockingCollection<Thing>();

                    var ackSentAmre = new AsyncManualResetEvent();
                    acceptedSubscription.AfterAcknowledgment += () => ackSentAmre.SetByAsyncCompletion();

                    acceptedSubscription.Subscribe(x =>
                    {
                        acceptedSusbscriptionList.Add(x);
                        Thread.Sleep(20);
                    });

                    await acceptedSubscription.StartAsync();

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
                            store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)
                            {
                                Strategy = SubscriptionOpeningStrategy.WaitForFree,
                                TimeToWaitBeforeConnectionRetryMilliseconds = 250
                            }))
                    {

                        waitingSubscription.Subscribe(x =>
                        {
                            waitingSubscriptionList.Add(x);
                        });

                        var startAsync = waitingSubscription.StartAsync();

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

                var lastChangeVector = (await store.Admin.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector ?? null;
                await CreateDocuments(store, 5);

                var subscriptionCreationParams = new SubscriptionCreationOptions()
                {
                    Criteria = new SubscriptionCriteria("Things"),
                    ChangeVector = lastChangeVector
                };

                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCreationParams);

                using (
                    var acceptedSubscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)))
                {
                    var acceptedSusbscriptionList = new BlockingCollection<Thing>();
                    var takingOverSubscriptionList = new BlockingCollection<Thing>();
                    long counter = 0;
                    acceptedSubscription.Subscribe(x =>
                    {
                        Interlocked.Increment(ref counter);
                        acceptedSusbscriptionList.Add(x);
                    });

                    var batchProccessedByFirstSubscription = new AsyncManualResetEvent();

                    acceptedSubscription.AfterAcknowledgment +=
                        () =>
                        {
                            if (Interlocked.Read(ref counter) == 5)
                                batchProccessedByFirstSubscription.SetByAsyncCompletion();
                        };

                    await acceptedSubscription.StartAsync();

                    Thing thing;

                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(acceptedSusbscriptionList.TryTake(out thing, 5000), "no doc");
                    }

                    Assert.True(await batchProccessedByFirstSubscription.WaitAsync(TimeSpan.FromSeconds(15)), "no ack");

                    Assert.False(acceptedSusbscriptionList.TryTake(out thing));

                    // open second subscription
                    using (var takingOverSubscription = store.AsyncSubscriptions.Open<Thing>(
                        new SubscriptionConnectionOptions(subsId)
                    {
                        
                        Strategy = SubscriptionOpeningStrategy.TakeOver
                    }))
                    {
                        takingOverSubscription.Subscribe(x => takingOverSubscriptionList.Add(x));
                        await takingOverSubscription.StartAsync();

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
    }
}