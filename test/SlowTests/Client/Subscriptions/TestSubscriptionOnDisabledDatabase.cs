using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class TestSubscriptionOnDisabledDatabase:RavenTestBase
    {
        public TestSubscriptionOnDisabledDatabase(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(20);

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded, Skip = "https://issues.hibernatingrhinos.com/issue/RavenDB-21549")]
        public async Task Run(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Subscriptions.Create<User>(options: new SubscriptionCreationOptions()
                {
                    Name = "Subs1"
                });

                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions("Subs1") {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5),
                    MaxErroneousPeriod = TimeSpan.Zero
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

                var mre = new AsyncManualResetEvent();

                subscription.AfterAcknowledgment += batch =>
                {
                    if (names.Count != 0 && names.Count % 30 == 0)
                        mre.Set();
                    return Task.CompletedTask;
                };
              
                Assert.True(await mre.WaitAsync(_reasonableWaitTime));
                mre.Reset();

                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));

                var aggregateException = await Assert.ThrowsAsync<AggregateException>(async () => await subscriptionTask);
                var actualExceptionWasThrown = false;
                var subscriptionInvalidStateExceptionWasThrown = false;
                foreach (var e in aggregateException.InnerExceptions)
                {
                    if (e is SubscriptionInvalidStateException)
                    {
                        subscriptionInvalidStateExceptionWasThrown = true;
                    }
                    if (e is DatabaseDisabledException)
                    {
                        actualExceptionWasThrown = true;
                    }

                    if (subscriptionInvalidStateExceptionWasThrown && actualExceptionWasThrown)
                        break;
                }
                Assert.True(subscriptionInvalidStateExceptionWasThrown && actualExceptionWasThrown);

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
                    Assert.True(await mre.WaitAsync(_reasonableWaitTime));
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
