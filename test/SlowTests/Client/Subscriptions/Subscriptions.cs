using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using FastTests;
using Sparrow;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class SubscriptionsSlow : SubscriptionTestBase
    {

        [Fact]
        public async Task SubscriptionSimpleTakeOverStrategy()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastChangeVector = (await store.Admin.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector;
                await CreateDocuments(store, 5);

                var subscriptionCreationParams = new SubscriptionCreationOptions
                {
                    Criteria = new SubscriptionCriteria("Things"),
                    ChangeVector = lastChangeVector
                };
                var subsId = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                using (
                    var acceptedSubscription = store.Subscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)))
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

                    GC.KeepAlive(acceptedSubscription.Run(x =>
                    {
                        Interlocked.Increment(ref counter);
                         foreach (var item in x.Items)
                        {
                            acceptedSusbscriptionList.Add(item.Result);
                        }
                    }));

                    Thing thing;

                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(acceptedSusbscriptionList.TryTake(out thing, 5000), "no doc");
                    }

                    Assert.True(await batchProccessedByFirstSubscription.WaitAsync(TimeSpan.FromSeconds(15)), "no ack");

                    Assert.False(acceptedSusbscriptionList.TryTake(out thing));

                    // open second subscription
                    using (var takingOverSubscription = store.Subscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)
                    {
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