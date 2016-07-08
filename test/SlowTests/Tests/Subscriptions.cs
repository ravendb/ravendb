using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Tests.Notifications;
using Xunit;

namespace SlowTests.Tests
{
    public class Subscriptions: RavenTestBase
    {
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
                using (var subscription = store.AsyncSubscriptions.Open<Thing>(new SubscriptionConnectionOptions
                {
                    SubscriptionId = subsId
                }))
                {

                    var bc = new BlockingCollection<Thing>();

                    subscription.Subscribe(x =>
                    {
                        AsyncHelpers.RunSync(() => Task.Delay(100));
                        bc.Add(x);
                    });

                    subscription.Start();

                    Thing thing;
                    for (var i = 0; i < 5; i++)
                    {
                        Assert.True(bc.TryTake(out thing, 1000));
                    }

                    Assert.False(bc.TryTake(out thing, 50));

                    for (var j = 0; j < 2; j++)
                    {
                        await CreateDocuments(store, 1);

                        for (var i = 0; i < 5; i++)
                        {
                            Assert.True(bc.TryTake(out thing, 1000));
                        }

                        Assert.False(bc.TryTake(out thing, 50));
                    }
                }
            }
        }

        public class Thing
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
