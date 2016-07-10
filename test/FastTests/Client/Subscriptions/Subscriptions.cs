using Raven.Client.Connection.Implementation;
using Raven.Client.Document;
using Raven.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Extensions;
using Xunit;
using Raven.Tests.Notifications;
using Sparrow;

namespace FastTests.Client.Subscriptions
{
    public class Subscriptions : SubscriptionTestBase
    {
        [Fact]
        public async Task CreateSubscription()
        {
            using (var store = await GetDocumentStore())
            {
                var subscriptionCriteria = new Raven.Abstractions.Data.SubscriptionCriteria
                {
                    Collection = "People",
                };
                var subsId = await store.AsyncSubscriptions.CreateAsync(subscriptionCriteria);

                var subscriptionsConfig = await store.AsyncSubscriptions.GetSubscriptionsAsync(0, 10);

                Assert.Equal(1, subscriptionsConfig.Count);
                Assert.Equal(subscriptionCriteria.Collection, subscriptionsConfig[0].Criteria.Collection);
                Assert.Equal(subscriptionCriteria.FilterJavaScript, subscriptionsConfig[0].Criteria.FilterJavaScript);
                Assert.Equal(0, subscriptionsConfig[0].AckEtag);
                Assert.Equal(subsId, subscriptionsConfig[0].SubscriptionId);
            }
        }

        [Fact]
        public async Task BasicSusbscriptionTest()
        {
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                await CreateDocuments(store, 1);

                var lastEtag = store.GetLastWrittenEtag() ?? 0;
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
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                await CreateDocuments(store, 1);

                var lastEtag = store.GetLastWrittenEtag() ?? 0;
                await CreateDocuments(store, 5);

                var subscriptionCriteria = new Raven.Abstractions.Data.SubscriptionCriteria
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
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                await CreateDocuments(store, 1);

                var lastEtag = store.GetLastWrittenEtag() ?? 0;
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

                        Assert.True(await ackSentAmre.WaitAsync(50000));

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
            using (var store = await GetDocumentStore().ConfigureAwait(false))
            {
                await CreateDocuments(store, 1);

                var lastEtag = store.GetLastWrittenEtag() ?? 0;
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

                    Assert.True(await batchProccessedByFirstSubscription.WaitAsync(5000), "no ack");

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