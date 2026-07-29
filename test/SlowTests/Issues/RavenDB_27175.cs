using System;
using System.Collections.Concurrent;
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

    [RavenFact(RavenTestCategory.Subscriptions)]
    public void WorkerStatusIsPartOfThePublicApi()
    {
        // tests can see Raven.Client internals, so the accessibility has to be asserted explicitly
        var status = typeof(SubscriptionWorker<Company>).GetProperty(nameof(SubscriptionWorker<Company>.Status));

        Assert.NotNull(status);
        Assert.True(status.GetMethod.IsPublic);

        var stateChanged = typeof(SubscriptionWorker<Company>).GetEvent(nameof(SubscriptionWorker<Company>.OnStateChanged));

        Assert.NotNull(stateChanged);
        Assert.True(stateChanged.AddMethod.IsPublic);
        Assert.True(stateChanged.RemoveMethod.IsPublic);

        Assert.True(typeof(SubscriptionWorkerStatus).IsPublic);
        Assert.True(typeof(SubscriptionWorkerState).IsPublic);
    }

    [RavenFact(RavenTestCategory.Subscriptions)]
    public async Task StatusFollowsTheWorkerFromConnectingThroughProcessing()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Company { Name = "RavenDB" });
                session.SaveChanges();
            }

            var name = await store.Subscriptions.CreateAsync<Company>();

            using (var worker = store.Subscriptions.GetSubscriptionWorker<Company>(name))
            using (var processed = new ManualResetEventSlim())
            {
                Assert.Equal(SubscriptionWorkerState.NotStarted, worker.Status.State);
                Assert.Null(worker.Status.Exception);

                var observed = new ConcurrentQueue<SubscriptionWorkerState>();
                var raisedBy = new ConcurrentQueue<object>();

                worker.OnStateChanged += (sender, status) =>
                {
                    raisedBy.Enqueue(sender);
                    observed.Enqueue(status.State);
                };

                var stateWhileProcessing = SubscriptionWorkerState.NotStarted;

                _ = worker.Run(_ =>
                {
                    stateWhileProcessing = worker.Status.State;
                    processed.Set();
                });

                Assert.True(processed.Wait(_reasonableWaitTime), "the batch was never handed to the subscriber");

                Assert.Equal(SubscriptionWorkerState.Processing, stateWhileProcessing);

                // once the subscriber returned and the batch was acknowledged, the worker is idle again
                Assert.Equal(SubscriptionWorkerState.WaitingForDocuments,
                    await WaitForValueAsync(() => worker.Status.State, SubscriptionWorkerState.WaitingForDocuments));

                Assert.Equal(new[]
                {
                    SubscriptionWorkerState.Connecting,
                    SubscriptionWorkerState.WaitingForDocuments,
                    SubscriptionWorkerState.Processing
                }, observed.Take(3).ToArray());

                Assert.NotEmpty(raisedBy);
                Assert.All(raisedBy, sender => Assert.Same(worker, sender));
            }
        }
    }

    [RavenFact(RavenTestCategory.Subscriptions)]
    public async Task StatusReportsRetryingWithTheFailureThatCausedIt()
    {
        using (var store = GetDocumentStore())
        {
            var name = await store.Subscriptions.CreateAsync<Company>();

            using (var worker = store.Subscriptions.GetSubscriptionWorker<Company>(new SubscriptionWorkerOptions(name)
                   {
                       // long enough that Retrying is still the state while the worker waits out the delay
                       TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(1)
                   }))
            using (var connected = new ManualResetEventSlim())
            {
                var retrying = new TaskCompletionSource<SubscriptionWorkerStatus>(TaskCreationOptions.RunContinuationsAsynchronously);

                worker.OnStateChanged += (_, status) =>
                {
                    switch (status.State)
                    {
                        case SubscriptionWorkerState.WaitingForDocuments:
                            connected.Set();
                            break;

                        case SubscriptionWorkerState.Retrying:
                            retrying.TrySetResult(status);
                            break;
                    }
                };

                // no documents are ever stored here, so the state is the only indication of what the worker is doing
                _ = worker.Run(_ => { });

                Assert.True(connected.Wait(_reasonableWaitTime), "initial connection wasn't established");
                Assert.Equal(SubscriptionWorkerState.WaitingForDocuments, worker.Status.State);
                Assert.Null(worker.Status.Exception);

                await DropConnectionAsync(store, name);

                var status = await retrying.Task.WaitAsync(_reasonableWaitTime);

                Assert.NotNull(status.Exception);
                Assert.Contains($"Simulating a broken connection for '{name}'", status.Exception.ToString());

                // and the worker recovers on its own, clearing the failure
                Assert.Equal(SubscriptionWorkerState.WaitingForDocuments,
                    await WaitForValueAsync(() => worker.Status.State, SubscriptionWorkerState.WaitingForDocuments));

                Assert.Null(worker.Status.Exception);
            }
        }
    }

    [RavenFact(RavenTestCategory.Subscriptions)]
    public async Task StatusIsFaultedWithTheFailureWhenTheWorkerGivesUp()
    {
        using (var store = GetDocumentStore())
        using (var worker = store.Subscriptions.GetSubscriptionWorker<Company>(new SubscriptionWorkerOptions("no-such-subscription")))
        {
            var ex = await Assert.ThrowsAsync<SubscriptionDoesNotExistException>(() => worker.Run(_ => { }));

            Assert.Equal(SubscriptionWorkerState.Faulted, worker.Status.State);
            Assert.Same(ex, worker.Status.Exception);
        }
    }

    [RavenFact(RavenTestCategory.Subscriptions)]
    public async Task StatusIsStoppedAfterDispose()
    {
        using (var store = GetDocumentStore())
        {
            var name = await store.Subscriptions.CreateAsync<Company>();

            var worker = store.Subscriptions.GetSubscriptionWorker<Company>(name);
            using (var connected = new ManualResetEventSlim())
            {
                worker.OnEstablishedSubscriptionConnection += () => connected.Set();

                _ = worker.Run(_ => { });

                Assert.True(connected.Wait(_reasonableWaitTime), "initial connection wasn't established");

                await worker.DisposeAsync();

                Assert.Equal(SubscriptionWorkerState.Stopped, worker.Status.State);
                Assert.Null(worker.Status.Exception);
            }
        }
    }

    [RavenFact(RavenTestCategory.Subscriptions)]
    public async Task StatusIsStoppedAfterDisposingAWorkerThatWasNeverRun()
    {
        using (var store = GetDocumentStore())
        {
            var name = await store.Subscriptions.CreateAsync<Company>();

            var worker = store.Subscriptions.GetSubscriptionWorker<Company>(name);

            Assert.Equal(SubscriptionWorkerState.NotStarted, worker.Status.State);

            await worker.DisposeAsync();

            Assert.Equal(SubscriptionWorkerState.Stopped, worker.Status.State);
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
