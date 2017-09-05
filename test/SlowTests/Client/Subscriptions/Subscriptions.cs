using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client.Subscriptions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Extensions;
using Sparrow;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class SubscriptionsSlow : SubscriptionTestBase
    {
        [Fact]
        public async Task SubscriptionSimpleTakeOverStrategy()
        {
            var timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(5);

            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastChangeVector = (await store.Admin.SendAsync(new GetStatisticsOperation())).DatabaseChangeVector;
                await CreateDocuments(store, 5);

                var subscriptionCreationParams = new SubscriptionCreationOptions
                {
                    Query = "from Things",
                    ChangeVector = lastChangeVector
                };
                var subsId = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                Task firstSubscription;
                using (var acceptedSubscription = store.Subscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)))
                {
                    var acceptedSubscriptionList = new BlockingCollection<Thing>();
                    long counter = 0;
                    var batchProcessedByFirstSubscription = new AsyncManualResetEvent();

                    acceptedSubscription.AfterAcknowledgment += b =>
                    {
                        if (Interlocked.Read(ref counter) == 5)
                            batchProcessedByFirstSubscription.Set();
                        return Task.CompletedTask;
                    };

                    firstSubscription = acceptedSubscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            Interlocked.Increment(ref counter);
                            acceptedSubscriptionList.Add(item.Result);
                        }
                    });

                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(acceptedSubscriptionList.TryTake(out _, timeout), "no doc");
                    }
                    Assert.True(await batchProcessedByFirstSubscription.WaitAsync(TimeSpan.FromSeconds(15)), "no ack");
                    Assert.False(acceptedSubscriptionList.TryTake(out _));
                }

                // open second subscription
                using (var takingOverSubscription = store.Subscriptions.Open<Thing>(new SubscriptionConnectionOptions(subsId)
                {
                    Strategy = SubscriptionOpeningStrategy.TakeOver
                }))
                {
                    var takingOverSubscriptionList = new BlockingCollection<Thing>();

                    GC.KeepAlive(takingOverSubscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            takingOverSubscriptionList.Add(item.Result);
                        }
                    }));

                    // Wait for the first subscription to finish before creating the documents.
                    await firstSubscription;
                    await CreateDocuments(store, 5);

                    // wait until we know that connection was established
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(takingOverSubscriptionList.TryTake(out _, timeout), "no doc takeover");
                    }
                    Assert.False(takingOverSubscriptionList.TryTake(out _));
                }
            }
        }
    }
}
