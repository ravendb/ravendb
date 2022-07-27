using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using FastTests;
using Orders;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_13761 : RavenTestBase
    {
        public RavenDB_13761(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(30);

        [Fact]
        public void DeleteRevisionShouldReturnNullCurrent()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();
                store.Maintenance.Send(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = false,
                        MinimumRevisionsToKeep = 5,
                        MinimumRevisionAgeToKeep = TimeSpan.FromDays(14)
                    }
                }));

                var existingSubscriptions = store.Subscriptions.GetSubscriptions(0, 1024);
                foreach (var state in existingSubscriptions)
                    store.Subscriptions.Delete(state.SubscriptionName, "Northwind");

                var subscriptionName = store.Subscriptions.Create(new SubscriptionCreationOptions<Revision<Order>>());

                using (var session = store.OpenSession())
                {
                    Order entity = new Order();
                    session.Store(entity, "orders/1");
                    session.SaveChanges();
                }

                var mre = new ManualResetEvent(false);
                using (var subscriptionWorker = store.Subscriptions
                    .GetSubscriptionWorker<Revision<Order>>(subscriptionName))
                {
                    var cts = new CancellationTokenSource();
                    subscriptionWorker.Run(batch =>
                    {

                        foreach (var batchItem in batch.Items)
                        {
                            if (batchItem.Result.Previous != null &&
                                batchItem.Result.Current == null) //we have a delete!
                            {

                                mre.Set();
                            }
                        }
                    }, cts.Token);

                    using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                    {
                        session.Delete("orders/1");
                        session.SaveChanges();
                    }
                    Assert.True(mre.WaitOne(_reasonableWaitTime));                    
                }
            }
        }

    }
}
