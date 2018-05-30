using System;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_4645 : RavenTestBase
    {
        [Fact]
        public void ShouldStopPullingTaskWhenSubscriptionIsDeleted()
        {
            using (var store = GetDocumentStore())
            {
                // insert few documents and fetch them using subscription
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Company());
                    }

                    session.SaveChanges();
                }

                var id = store.Subscriptions.Create<Company>();

                var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                {
                    MaxDocsPerBatch =  5,
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                var docs = new CountdownEvent(10);

                subscription.Run(batch=> batch.Items.ForEach(x =>
                {
                    docs.Signal();
                }));

                Assert.True(docs.Wait(TimeSpan.FromSeconds(15)));

                // all documents were fetched - time to delete subscription

                store.Subscriptions.Delete(id);

                // verify if we don't get new items

                docs.Reset(1);
                
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 2; i++)
                    {
                        session.Store(new Company());
                    }

                    session.SaveChanges();
                }

                // wait a bit for new documents - we shouldn't get any
                Assert.False(docs.Wait(50));
            }
        }
    }
}
