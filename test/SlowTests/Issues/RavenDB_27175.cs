using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Issues;

public class RavenDB_27175(ITestOutputHelper output) : RavenTestBase(output)
{
    private readonly TimeSpan _reasonableWaitTime = TimeSpan.FromSeconds(60);

    [RavenFact(RavenTestCategory.Subscriptions)]
    public void OnEstablishedSubscriptionConnectionIsPartOfThePublicApi()
    {
        // tests can see Raven.Client internals, so the accessibility has to be asserted explicitly
        var @event = typeof(SubscriptionWorker<Company>).GetEvent(nameof(SubscriptionWorker<Company>.OnEstablishedSubscriptionConnection));

        Assert.NotNull(@event);
        Assert.True(@event.AddMethod.IsPublic);
        Assert.True(@event.RemoveMethod.IsPublic);
    }

    [RavenFact(RavenTestCategory.Subscriptions)]
    public async Task CanBeNotifiedOnReconnectionOfAnIdleSubscription()
    {
        using (var store = GetDocumentStore())
        {
            var name = await store.Subscriptions.CreateAsync<Company>();

            using (var worker = store.Subscriptions.GetSubscriptionWorker<Company>(new SubscriptionWorkerOptions(name)
                   {
                       TimeToWaitBeforeConnectionRetry = TimeSpan.FromMilliseconds(16)
                   }))
            using (var firstConnection = new ManualResetEventSlim())
            using (var reconnection = new ManualResetEventSlim())
            using (var retry = new ManualResetEventSlim())
            {
                var connections = 0;

                worker.OnEstablishedSubscriptionConnection += () =>
                {
                    if (Interlocked.Increment(ref connections) == 1)
                        firstConnection.Set();
                    else
                        reconnection.Set();
                };

                worker.OnSubscriptionConnectionRetry += _ => retry.Set();

                // no documents are ever stored here, so the events are the only indication that we are connected
                _ = worker.Run(_ => { });

                Assert.True(firstConnection.Wait(_reasonableWaitTime), "initial connection wasn't established");

                await DropConnectionAsync(store, name);

                Assert.True(retry.Wait(_reasonableWaitTime), "OnSubscriptionConnectionRetry wasn't raised");
                Assert.True(reconnection.Wait(_reasonableWaitTime), "OnEstablishedSubscriptionConnection wasn't raised on reconnection");
            }
        }
    }

    private async Task DropConnectionAsync(Raven.Client.Documents.IDocumentStore store, string subscriptionName)
    {
        var db = await Databases.GetDocumentDatabaseInstanceFor(store, store.Database);
        using (db.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
        using (ctx.OpenReadTransaction())
        {
            var connectionsState = db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, subscriptionName);
            Assert.NotNull(connectionsState);
            Assert.NotEmpty(connectionsState.GetConnections().ToList());

            connectionsState.DropSubscription(new SubscriptionClosedException($"Simulating a broken connection for '{subscriptionName}'", canReconnect: true));
        }
    }
}
