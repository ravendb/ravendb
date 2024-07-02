using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_9068 : RavenTestBase
    {
        public RavenDB_9068(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromSeconds(60 * 10) : TimeSpan.FromSeconds(50);
        [Fact]
        public async Task CancellingPassedCancellationTokenToRunShouldCancelSubscriptionExecution()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User());
                    await session.SaveChangesAsync();
                }

                var subscriptionId = await store.Subscriptions.CreateAsync<User>();
                var subscription = store.Subscriptions.GetSubscriptionWorker<User>(subscriptionId);
                var cts = new CancellationTokenSource();
                var subscriptionTask = subscription.Run(x => { }, cts.Token);
                cts.Cancel();

                var task = Assert.ThrowsAnyAsync<Exception>(() => subscriptionTask);
                Assert.True(await task.WaitWithoutExceptionAsync(_reasonableWaitTime), "1. await task.WaitWithoutExceptionAsync(_reasonableWaitTime)");
                var e = await task;
                Assert.True(e is TaskCanceledException or OperationCanceledException or AggregateException { InnerException: TaskCanceledException }, $"1. e is TaskCanceledException or OperationCanceledException or AggregateException {{ InnerException: TaskCanceledException }}, {e}");

                subscription = store.Subscriptions.GetSubscriptionWorker<User>(subscriptionId);
                cts = new CancellationTokenSource();
                subscriptionTask = subscription.Run(x => Task.CompletedTask, cts.Token);
                cts.Cancel();

                task = Assert.ThrowsAnyAsync<Exception>(() => subscriptionTask);
                Assert.True(await task.WaitWithoutExceptionAsync(_reasonableWaitTime), "2. await task.WaitWithoutExceptionAsync(_reasonableWaitTime)");
                e = await task;
                Assert.True(e is TaskCanceledException or OperationCanceledException or AggregateException { InnerException: TaskCanceledException }, $"2. e is TaskCanceledException or OperationCanceledException or AggregateException {{ InnerException: TaskCanceledException }}, {e}");
            }
        }
    }
}
