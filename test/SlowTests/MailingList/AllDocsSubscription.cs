using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Nito.AsyncEx;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;
using AsyncManualResetEvent = Sparrow.Server.AsyncManualResetEvent;

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

        [Fact]
        public async Task CanUseAllDocsSubscriptionWithRevisions()
        {
            using var store = GetDocumentStore();
            await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
            {
                Name = "Test",
                Query = "from @all_docs (Revisions = true)"
            });

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 5,
                    },
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Cats"] = new RevisionsCollectionConfiguration
                        {
                            Disabled = false
                        },
                        ["Dogs"] = new RevisionsCollectionConfiguration
                        {
                            Disabled = false
                        }
                    }
                };

                await Server.ServerStore.ModifyDatabaseRevisions(context,
                    store.Database,
                    DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(configuration,
                        context), Guid.NewGuid().ToString());
            }

            for (int i = 0; i < 5; i++)
            {
                for (var j = 0; j < 5; j++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Dog
                        {
                            Name = $"Arrava {i} ver {j}"
                        }, "dogs/" + i);

                        session.Store(new Cat()
                        {
                            Name = $"Mitzi {i} ver {j}"
                        }, "cats/" + i);

                        session.SaveChanges();
                    }
                }
            }

            var subscription = store.Subscriptions.GetSubscriptionWorker<Revision<Animal>>(new SubscriptionWorkerOptions("Test"));
            var names = new BlockingCollection<string>();
            var cde = new AsyncCountdownEvent(50);
            var t = subscription.Run(batch =>
            {
                foreach (var item in batch.Items)
                {
                    if (item.Result.Current.Name != null)// also getting hilo docs here
                        names.Add(item.Result.Current.Name);
                    cde.Signal();
                }
            });

            using (var cte = new CancellationTokenSource())
            {
                cte.CancelAfter(50000);
                await cde.WaitAsync(cte.Token);
            }


            await subscription.DisposeAsync();
            await t;

            Assert.Equal(50, names.Count);

        }
        public AllDocsSubscription(ITestOutputHelper output) : base(output)
        {
        }
    }
}
