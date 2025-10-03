using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Tests.Infrastructure;
using Sparrow.Server;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Issues
{
    public class RavenDB_17624 : RavenTestBase
    {
        public RavenDB_17624(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ForbidOpeningMoreThenOneSessionPerSubscriptionBatch(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Command { Value = 1 });
                await session.StoreAsync(new Command { Value = 2 });
                await session.SaveChangesAsync();
            }

            try
            {
                await store.Subscriptions
                    .GetSubscriptionStateAsync("BackgroundSubscriptionWorker");
            }
            catch (SubscriptionDoesNotExistException)
            {
                await store.Subscriptions
                    .CreateAsync(new SubscriptionCreationOptions<Command>
                    {
                        Name = "BackgroundSubscriptionWorker",
                        Filter = x => x.ProcessedOn == null
                    });
            }

            var workerOptions = new SubscriptionWorkerOptions("BackgroundSubscriptionWorker");

            using var worker = store.Subscriptions
               .GetSubscriptionWorker<Command>(workerOptions);

            var mre = new AsyncManualResetEvent();
            var cts = new CancellationTokenSource();
            var last = DateTime.UtcNow;
            InvalidOperationException exception = null;
            var t = worker.Run(async batch =>
            {
                using (var session = batch.OpenAsyncSession())
                {
                }

                exception = await Assert.ThrowsAsync<InvalidOperationException>( () =>
                {
                    using var session = batch.OpenAsyncSession();
                    return Task.CompletedTask;
                });

                mre.Set();
            }, cts.Token);

            Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(15)));
            Assert.NotNull(exception);
            Assert.Equal("Session can only be opened once per each Subscription batch", exception.Message);
        }

        [RavenTheory(RavenTestCategory.Subscriptions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ClearSessionOpenedWhenBatchIsReused(Options options)
        {
            using var store = GetDocumentStore(options);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Command { Value = 1 });
                await session.StoreAsync(new Command { Value = 2 });
                await session.SaveChangesAsync();
            }

            await store.Subscriptions
                .CreateAsync(new SubscriptionCreationOptions<Command>
                {
                    Name = "BackgroundSubscriptionWorker",
                });

            var workerOptions = new SubscriptionWorkerOptions("BackgroundSubscriptionWorker")
            {
                MaxDocsPerBatch = 1,
                CloseWhenNoDocsLeft = true
            };

            await using var worker = store.Subscriptions
               .GetSubscriptionWorker<Command>(workerOptions);
            
            await Assert.ThrowsAsync<SubscriptionClosedException>(() => worker.Run(batch =>
            {
                using (var session = batch.OpenAsyncSession())
                {
                }
            }));
        }

        private class Command
        {
            public string Id { get; set; }

            public DateTime? ProcessedOn { get; set; }

            public int Value { get; set; }
        }
    }
}
