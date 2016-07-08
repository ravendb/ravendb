using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Xunit;
using System.Threading.Tasks;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Notifications;


namespace FastTests.Client.Subscriptions
{
    public class RavenDB_3193 : RavenTestBase
    {
        [Fact]
        public async Task ShouldRespectCollectionCriteria()
        {
            using (var store = await GetDocumentStore())
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

                var id = await store.AsyncSubscriptions.CreateAsync(new SubscriptionCriteria
                {
                    Collection = "Users"                    
                });

                using (var subscription = store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions
                {
                    MaxDocsPerBatch = 31,
                    SubscriptionId = id
                }))
                {
                    var docs = new List<RavenJObject>();

                    subscription.Subscribe(docs.Add);

                    subscription.Start();

                    SpinWait.SpinUntil(() => docs.Count >= 100, TimeSpan.FromSeconds(60));
                    Assert.Equal(100, docs.Count);


                    foreach (var jsonDocument in docs)
                    {
                        var collection =
                            jsonDocument[Constants.Metadata].Value<string>(Constants.Headers.RavenEntityName);
                        Assert.True(collection == "Users");
                    }
                }
            }
        }
    }
}
