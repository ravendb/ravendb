using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Nito.AsyncEx;
using Raven.Client.Documents.Subscriptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class AllDocsSubscription : RavenTestBase
    {
        private class Animal
        {
            public string Name;
        }
        private class Dog : Animal { }
        private class Cat : Animal { }

        [Fact]
        public async Task CanUseAllDocsSubscription()
        {
            using var store = GetDocumentStore();
            await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
            {
                Name = "Test",
                Query = "from @all_docs"
            });

            using (var s = store.OpenAsyncSession())
            {
                await s.StoreAsync(new Dog { Name = "Arava" });
                await s.StoreAsync(new Cat { Name = "Mitzi" });
                await s.SaveChangesAsync();
            }

            var subscription = store.Subscriptions.GetSubscriptionWorker<Animal>(new SubscriptionWorkerOptions("Test"));
            var names = new BlockingCollection<string>();
            var cde = new AsyncCountdownEvent(4);
            var t = subscription.Run(batch =>
            {
                foreach (var item in batch.Items)
                {
                    if(item.Result.Name != null)// also getting hilo docs here
                        names.Add(item.Result.Name);
                    cde.Signal();
                }
            });

            using (var cte = new CancellationTokenSource())
            {
                cte.CancelAfter(50000);
                await cde.WaitAsync(cte.Token);
            }

            cde = new AsyncCountdownEvent(2);

            using (var s = store.OpenAsyncSession())
            {
                await s.StoreAsync(new Dog { Name = "Phoebe" });
                await s.StoreAsync(new Cat { Name = "Puffball" });
                await s.SaveChangesAsync();
            }

            using (var cte = new CancellationTokenSource())
            {
                cte.CancelAfter(50000);
                await cde.WaitAsync(cte.Token);
            }

            await subscription.DisposeAsync();
            await t;

            Assert.Equal(4, names.Count);

        }

        public AllDocsSubscription(ITestOutputHelper output) : base(output)
        {
        }
    }
}
