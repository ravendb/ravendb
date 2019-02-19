using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Subscriptions;
using Xunit;

namespace SlowTests.Tests
{
    public class Subscriptions : RavenTestBase
    {
        [Fact]
        public async Task BasicSusbscriptionTest()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastChangeVector = store.Maintenance.Send(new GetStatisticsOperation()).DatabaseChangeVector;
                await CreateDocuments(store, 5);

                var subscriptionCreationParams = new SubscriptionCreationOptions
                {
                    Query = "from Things",
                    ChangeVector = lastChangeVector
                };

                var subsId = await store.Subscriptions.CreateAsync(subscriptionCreationParams).ConfigureAwait(false);
                using (var subscription = store.Subscriptions.GetSubscriptionWorker<Thing>(new SubscriptionWorkerOptions(subsId)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {

                    var bc = new BlockingCollection<Thing>();


                    GC.KeepAlive(subscription.Run(x =>
                    {
                        foreach (var item in x.Items)
                        {
                            bc.Add(item.Result);
                        }
                    }));

                    Thing thing;
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(bc.TryTake(out thing, 1000));
                    }

                    Assert.False(bc.TryTake(out thing, 50));

                    for (var j = 0; j < 2; j++)
                    {
                        await CreateDocuments(store, 1);

                        Assert.True(bc.TryTake(out thing, 500));
                        Assert.False(bc.TryTake(out thing, 50));
                    }
                }
            }
        }

        private class Thing
        {
            public string Name { get; set; }
        }

        private async Task CreateDocuments(DocumentStore store, int amount)
        {
            using (var session = store.OpenAsyncSession())
            {
                for (var i = 0; i < amount; i++)
                {
                    await session.StoreAsync(new Thing
                    {
                        Name = $"ThingNo{i}"
                    });
                }
                await session.SaveChangesAsync();
            }
        }
    }
}
