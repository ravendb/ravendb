using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client.Subscriptions
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

                var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                {
                    MaxDocsPerBatch =  5 
                });

                var docs = new List<dynamic>();

                subscription.Run(batch=> batch.Items.ForEach(x=>docs.Add(x.Result)));

                Assert.True(SpinWait.SpinUntil(() => docs.Count == 10, TimeSpan.FromSeconds(60)));

                // all documents were fetched - time to delete subscription

                store.Subscriptions.Delete(id);

                // verify if we don't get new items

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 2; i++)
                    {
                        session.Store(new Company());
                    }

                    session.SaveChanges();
                }

                // wait 3 seconds for new documents - we shouldn't get any
                Assert.False(SpinWait.SpinUntil(() => docs.Count != 10, TimeSpan.FromSeconds(3)));
            }
        }
    }
}
