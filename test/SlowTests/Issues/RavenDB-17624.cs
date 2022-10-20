using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Issues
{
    public class RavenDB_17624 : RavenTestBase
    {
        public RavenDB_17624(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ForbidOpeningMoreThenOneSessionPerSubscriptionBatch()
        {
            var store = GetDocumentStore();

            string id1;
            string id2;
            using (var session = store.OpenAsyncSession())
            {
                var c1 = new Command { Value = 1 };
                var c2 = new Command { Value = 2 };
                await session.StoreAsync(c1);
                await session.StoreAsync(c2);
                await session.SaveChangesAsync();

                id1 = c1.Id;
                id2 = c2.Id;
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

            worker.OnSubscriptionConnectionRetry += exception => Console.WriteLine(exception);
            worker.OnUnexpectedSubscriptionError += exception => Console.WriteLine(exception);

            var mre = new ManualResetEvent(false);
            var cts = new CancellationTokenSource();
            var last = DateTime.UtcNow;
            InvalidOperationException exception = null;
            var t = worker.Run(async batch =>
            {
                if (batch.Items.Count < 2)
                    return;

                var item0 = batch.Items[0];
                using (var session = batch.OpenAsyncSession())
                {
                    item0.Result.ProcessedOn = last;
                    await session.SaveChangesAsync();
                }

                var item1 = batch.Items[1];

                exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                {
                    using (var session = batch.OpenAsyncSession())
                    {
                        item1.Result.ProcessedOn = last;
                        await session.SaveChangesAsync();
                    }
                });


                mre.Set();
            }, cts.Token);


            Assert.True(mre.WaitOne(TimeSpan.FromSeconds(15)));
            Assert.NotNull(exception);
            Assert.Equal("'SubscriptionBatch' can open only 1 session", exception.Message);

            DateTime? pVal1 = null;
            DateTime? pVal2 = null;
            using (var session = store.OpenAsyncSession())
            {
                var c1 = await session.LoadAsync<Command>(id1);
                var c2 = await session.LoadAsync<Command>(id2);

                await session.SaveChangesAsync();

                pVal1 = c1.ProcessedOn;
                pVal2 = c2.ProcessedOn;
            }

            Assert.Equal(last, pVal1);
            Assert.Equal(null, pVal2);
        }

        public class Command
        {
            public string Id { get; set; }

            public DateTime? ProcessedOn { get; set; }

            public int Value { get; set; }
        }
    }
}
