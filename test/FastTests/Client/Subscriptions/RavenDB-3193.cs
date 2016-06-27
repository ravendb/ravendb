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
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        session.Store(new Company());
                        session.Store(new User());
                        session.Store(new Address());
                    }

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria
                {
                    Collection = "Users"                    
                });

                var subscription = store.Subscriptions.Open(id, new SubscriptionConnectionOptions
                {
                    MaxDocsPerBatch=31                    
                });

                var docs = new List<RavenJObject>();

                subscription.Subscribe(docs.Add);

                Assert.True(SpinWait.SpinUntil(() => docs.Count >= 100, TimeSpan.FromSeconds(60)));


                foreach (var jsonDocument in docs)
                {
                    var collection = jsonDocument[Constants.Metadata].Value<string>(Constants.Headers.RavenEntityName);
                    Assert.True(collection == "Users");
                }
            }
        }
    }
}
