using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Subscriptions
{
    public class TestSubscriptionOnDisabledDatabase:RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(20);

        [Fact]
        public async Task Run()
        {

            using (var store = GetDocumentStore())
            {
                store.Subscriptions.Create<User>(options: new SubscriptionCreationOptions()
                {
                    Name = "Subs1"
                });

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("Subs1") {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 30; i++)
                        session.Store(new User { Name = i.ToString() });
                    session.SaveChanges();
                }

                List<string> names = new List<string>();
                var subscriptionTask = subscription.Run(x =>
                {
                    foreach (var item in x.Items)
                    {
                        names.Add(item.Result.Name);
                    }
                });

                var mre = new ManualResetEvent(false);

                subscription.AfterAcknowledgment += batch =>
                {
                    if (names.Count != 0 && names.Count % 30 == 0)
                        mre.Set();
                    return Task.CompletedTask;
                };
              
                Assert.True(mre.WaitOne(_reasonableWaitTime));
                mre.Reset();

                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));

                await Assert.ThrowsAsync<DatabaseDisabledException>(async () => await subscriptionTask);

                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, false));


                subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("Subs1") {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                });

                subscription.AfterAcknowledgment += batch =>
                {
                    if (names.Count != 0 && names.Count % 30 == 0)
                        mre.Set();
                    return Task.CompletedTask;
                };

                using (var session = store.OpenSession())
                {
                    for (var i = 0; i < 30; i++)
                        session.Store(new User { Name = i.ToString() });
                    session.SaveChanges();
                }

                var t = subscription.Run(x =>
                {
                    foreach (var item in x.Items)
                    {
                        names.Add(item.Result.Name);
                    }
                });
               
                try
                {
                    Assert.True(mre.WaitOne(_reasonableWaitTime));
                }
                catch (Exception e)
                {
                    if (t.Exception == null)
                        throw;
                    throw new AggregateException(new[] {t.Exception, e});
                }
            }
        }
    }
}
