using System.Collections.Concurrent;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Notifications;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Operations.Databases;
using Xunit;

namespace SlowTests.Tests
{
    public class Subscriptions : RavenNewTestBase
    {
        [Fact]
        public async Task BasicSusbscriptionTest()
        {
            using (var store = GetDocumentStore())
            {
                await CreateDocuments(store, 1);

                var lastEtag = store.Admin.Send(new GetStatisticsOperation()).LastDocEtag ?? 0;
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
                        bc.Add(x);
                    });

                    await subscription.StartAsync();

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
