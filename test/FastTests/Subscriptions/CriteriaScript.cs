using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using FastTests.Subscriptions;
using Raven.Client.Data;
using Raven.Client.Document;
using Raven.Client.Operations.Databases;
using Xunit;

namespace NewClientTests.NewClient.Subscriptions
{
    public class CriteriaScript: SubscriptionTestBase
    {
        [Fact]
        public async Task BasicCriteriaTest()
        {
            using (var store = GetDocumentStore())
            using (var subscriptionManager = new DocumentSubscriptions(store))
            {
                await CreateDocuments(store, 1);

                var lastEtag = (await store.Admin.SendAsync(new GetStatisticsOperation())).LastDocEtag ?? 0;
                await CreateDocuments(store, 5);

                var subscriptionCriteria = new SubscriptionCriteria
                {
                    Collection = "Things",
                    FilterJavaScript = " return this.Name == 'ThingNo3'",
                };
                var subsId = subscriptionManager.Create(subscriptionCriteria, lastEtag);
                using (var subscription = subscriptionManager.Open<Thing>(new SubscriptionConnectionOptions()
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
                    Assert.True(list.TryTake(out thing, 5000));
                    Assert.Equal("ThingNo3", thing.Name);
                    Assert.False(list.TryTake(out thing, 50));
                }
            }
        }
    }
}
