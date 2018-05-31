using System;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_3193 : RavenTestBase
    {
        [Fact]
        public void ShouldRespectCollectionCriteria()
        {
            using (var store = GetDocumentStore())
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

                var id = store.Subscriptions.Create<User>();

                var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    MaxDocsPerBatch = 31,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                var docs = new CountdownEvent(100);

                subscription.Run(x=>
                {
                    foreach (var item in x.Items)
                    {
                        var collection = item.Metadata[Raven.Client.Constants.Documents.Metadata.Collection];
                        if (collection.Equals("Users"))
                            docs.Signal();
                    }
                });

                Assert.True(docs.Wait(TimeSpan.FromSeconds(60)));                 
            }
        }
    }
}
