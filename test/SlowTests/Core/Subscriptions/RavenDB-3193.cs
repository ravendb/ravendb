using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using FastTests.Server.Documents.Notifications;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Core.Subscriptions
{
    public class RavenDB_3193 : RavenTestBase
    {
        [Fact]
        public async Task ShouldRespectCollectionCriteria()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await session.StoreAsync(new Company());
                        await session.StoreAsync(new User());
                        await session.StoreAsync(new Address());
                    }

                    await session.SaveChangesAsync();
                }

                var subscriptionCreationParams = new SubscriptionCreationOptions
                {
                    Criteria = new SubscriptionCriteria("Users")
                };
                var id = await store.AsyncSubscriptions.CreateAsync(subscriptionCreationParams);

                using (var subscription = store.AsyncSubscriptions.Open(
                    new SubscriptionConnectionOptions(id){
                        MaxDocsPerBatch = 31
                    }))
                {
                    var docs = new List<dynamic>();

                    subscription.Subscribe(docs.Add);

                    await subscription.StartAsync();

                    SpinWait.SpinUntil(() => docs.Count >= 100, TimeSpan.FromSeconds(60));
                    Assert.Equal(100, docs.Count);
                    foreach (var doc in docs)
                    {
                        Assert.True(doc.Id.StartsWith("users/"));
                    }
                }
            }
        }
    }
}
