using System.Collections.Concurrent;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Xunit;
using System;
using System.Threading;
using FastTests.Subscriptions;
using Raven.NewClient.Client.Exceptions.Subscriptions;
using Sparrow;

namespace NewClientTests.NewClient.Subscriptions
{
    public class Subscriptions : SubscriptionTestBase
    {
        [Fact]
        public async Task CreateSubscriptions()
        {
            var store = GetDocumentStore();
            
            var subscriptionCriteria = new SubscriptionCriteria
            {
                Collection = "People",
            };
            var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCriteria);

            var subscriptionCriteria2 = new SubscriptionCriteria
            {
                Collection = "Users",
            };
            var subsId2 = await store.AsyncSubscriptions.CreateAsync(subscriptionCriteria2);

            var subscriptionsConfig = await store.AsyncSubscriptions.GetSubscriptionsAsync(0, 10);

            Assert.Equal(2, subscriptionsConfig.Count);
            Assert.Equal(subscriptionCriteria.Collection, subscriptionsConfig[0].Criteria.Collection);
            Assert.Equal(subscriptionCriteria2.FilterJavaScript, subscriptionsConfig[1].Criteria.FilterJavaScript);
            Assert.Equal(0, subscriptionsConfig[0].AckEtag);
            Assert.Equal(subsId2, subscriptionsConfig[1].SubscriptionId);

            // TODO fix delete handler on server side
            /*await store.AsyncSubscriptions.DeleteAsync(subsId);
            subscriptionsConfig = await store.AsyncSubscriptions.GetSubscriptionsAsync(0, 10);
            Assert.Equal(1, subscriptionsConfig.Count);*/
        }

        [Fact]
        public async Task BasicSusbscriptionTest()
        {
            var store = GetDocumentStore();
            
            await CreateDocuments(store, 1);
            long lastEtag;
            using (var session = store.OpenAsyncSession())
            {
                var thing = await session.LoadAsync<Thing>("things/1");
                lastEtag = session.Advanced.GetEtagFor(thing) ?? 0;
            }
            await CreateDocuments(store, 5);

            var subscriptionCriteria = new SubscriptionCriteria
            {
                Collection = "Things",
            };
            var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCriteria, lastEtag);
            using (var subscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions()
            {
                SubscriptionId = subsId
            }))
            {
                var list = new BlockingCollection<Thing>();
                subscription.Subscribe(x =>
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

        [Fact]
        public async Task SubscriptionStrategyConnectIfFree()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                long lastEtag;
                using (var session = store.OpenAsyncSession())
                {
                    var thing = await session.LoadAsync<Thing>("things/1");
                    lastEtag = session.Advanced.GetEtagFor(thing) ?? 0;
                }
                await CreateDocuments(store, 5);

                var subscriptionCriteria = new SubscriptionCriteria
                {
                    Collection = "Things",
                };
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCriteria, lastEtag);
                using (
                    var acceptedSubscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions()
                    {
                        SubscriptionId = subsId,
                        TimeToWaitBeforeConnectionRetryMilliseconds = 10000
                    }))
                {

                    var acceptedSusbscriptionList = new BlockingCollection<Thing>();
                    acceptedSubscription.Subscribe(x =>
                    {
                        acceptedSusbscriptionList.Add(x);
                    });
                    await acceptedSubscription.StartAsync();

                    Thing thing;

                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(acceptedSusbscriptionList.TryTake(out thing, 1000));
                    }
                    Assert.False(acceptedSusbscriptionList.TryTake(out thing, 50));

                    // open second subscription
                    using (
                        var rejectedSusbscription =
                            store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions()
                            {
                                SubscriptionId = subsId,
                                Strategy = SubscriptionOpeningStrategy.OpenIfFree,
                                TimeToWaitBeforeConnectionRetryMilliseconds = 6000
                            }))
                    {

                        rejectedSusbscription.Subscribe(thing1 => { });

                        await Assert.ThrowsAsync<SubscriptionInUseException>(async () => await rejectedSusbscription.StartAsync());

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

                long lastEtag;
                using (var session = store.OpenAsyncSession())
                {
                    var thing = await session.LoadAsync<Thing>("things/1");
                    lastEtag = session.Advanced.GetEtagFor(thing) ?? 0;
                }
                await CreateDocuments(store, 5);

                var subscriptionCriteria = new SubscriptionCriteria
                {
                    Collection = "Things",
                };
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCriteria, lastEtag);
                using (
                    var acceptedSubscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions()
                    {
                        SubscriptionId = subsId
                    }))
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
                            store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions()
                            {
                                SubscriptionId = subsId,
                                Strategy = SubscriptionOpeningStrategy.WaitForFree,
                                TimeToWaitBeforeConnectionRetryMilliseconds = 250
                            }))
                    {

                        waitingSubscription.Subscribe(x =>
                        {
                            waitingSubscriptionList.Add(x);
                        });
                        var taskStarted = waitingSubscription.StartAsync();
                        var completed = await Task.WhenAny(taskStarted, Task.Delay(300));


                        Assert.False(completed == taskStarted);

                        Assert.True(await ackSentAmre.WaitAsync(TimeSpan.FromSeconds(50)));

                        acceptedSubscription.Dispose();

                        await CreateDocuments(store, 5);

                        // wait until we know that connection was established
                        for (var i = 0; i < 5; i++)
                        {
                            Assert.True(waitingSubscriptionList.TryTake(out thing, 1000));
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

                long lastEtag;
                using (var session = store.OpenAsyncSession())
                {
                    var thing = await session.LoadAsync<Thing>("things/1");
                    lastEtag = session.Advanced.GetEtagFor(thing) ?? 0;
                }
                await CreateDocuments(store, 5);

                var subscriptionCriteria = new SubscriptionCriteria
                {
                    Collection = "Things",
                };
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCriteria, lastEtag);
                using (
                    var acceptedSubscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions()
                    {
                        SubscriptionId = subsId
                    }))
                {
                    var acceptedSusbscriptionList = new BlockingCollection<Thing>();
                    var takingOverSubscriptionList = new BlockingCollection<Thing>();

                    acceptedSubscription.Subscribe(x =>
                    {
                        acceptedSusbscriptionList.Add(x);
                    });

                    var batchProccessedByFirstSubscription = new AsyncManualResetEvent();

                    acceptedSubscription.AfterAcknowledgment +=
                        () => batchProccessedByFirstSubscription.SetByAsyncCompletion();

                    await acceptedSubscription.StartAsync();

                    Thing thing;

                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(acceptedSusbscriptionList.TryTake(out thing, 5000), "no doc");
                    }

                    Assert.True(await batchProccessedByFirstSubscription.WaitAsync(TimeSpan.FromSeconds(5)), "no ack");
                    
                    Assert.False(acceptedSusbscriptionList.TryTake(out thing));

                    // open second subscription
                    using (var takingOverSubscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions()
                    {
                        SubscriptionId = subsId,
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