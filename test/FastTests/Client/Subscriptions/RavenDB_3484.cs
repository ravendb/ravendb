using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Xunit;
using Xunit.Extensions;

namespace FastTests.Client.Subscriptions
{
    public class RavenDB_3484 : RavenTestBase
    {
        private readonly TimeSpan waitForDocTimeout = TimeSpan.FromSeconds(20);

        [Fact]
        public void OpenIfFree_ShouldBeDefaultStrategy()
        {
            Assert.Equal(SubscriptionOpeningStrategy.OpenIfFree, new SubscriptionConnectionOptions("test").Strategy);
        }

        [Fact]
        public async Task ShouldRejectWhen_OpenIfFree_StrategyIsUsed()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                var subscription = store.Subscriptions.Open(new SubscriptionConnectionOptions(id));

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var mre = new AsyncManualResetEvent();
                subscription.Run(x => mre.Set());

                Assert.True(await mre.WaitAsync(TimeSpan.FromSeconds(60)));

                await Assert.ThrowsAsync<SubscriptionInUseException>(() => store.Subscriptions.Open(new SubscriptionConnectionOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.OpenIfFree
                }).Run(x => { }));
            }
        }

        [Fact]
        public async Task ShouldReplaceActiveClientWhen_TakeOver_StrategyIsUsed()
        {
            using (var store = GetDocumentStore())
            {
                Server.ServerStore.Observer.Suspended = true;
                var id = store.Subscriptions.Create<User>();

                const int numberOfClients = 2;

                var subscriptions = new (Subscription<User> Subscription, Task Task, BlockingCollection<User> Items)[numberOfClients];

                try
                {
                    for (int i = 0; i < numberOfClients; i++)
                    {
                        var subscriptionOpeningStrategy = i > 0 ? SubscriptionOpeningStrategy.TakeOver : SubscriptionOpeningStrategy.OpenIfFree;
                        var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                        {
                            Strategy = subscriptionOpeningStrategy
                        });

                        var items = new BlockingCollection<User>();
                    
                        var subscriptionRunningTask = subscription.Run(x=>
                        {
                            foreach (var item in x.Items)
                            {
                                items.Add(item.Result);
                            }
                        });

                        if (i > 0)
                        {
                            Assert.True(SpinWait.SpinUntil(() => subscriptions[i - 1].Task.IsCompleted, TimeSpan.FromSeconds(60)));
                            await Assert.ThrowsAsync<SubscriptionInUseException>(()=>subscriptions[i - 1].Task);
                        }

                        subscriptions[i] = (subscription,subscriptionRunningTask, items);

                        var batchAcknowledged = false;

                        subscription.AfterAcknowledgment += x =>
                        {
                            batchAcknowledged = true;
                            return Task.CompletedTask;
                        };

                        using (var s = store.OpenSession())
                        {
                            s.Store(new User());
                            s.Store(new User());

                            s.SaveChanges();
                        }

                        Assert.True(subscriptions[i].Items.TryTake(out _, waitForDocTimeout));
                        Assert.True(subscriptions[i].Items.TryTake(out _, waitForDocTimeout));

                        SpinWait.SpinUntil(() => batchAcknowledged, TimeSpan.FromSeconds(5)); // let it acknowledge the processed batch before we open another subscription

                        if (i > 0)
                        {
                            Assert.False(subscriptions[i - 1].Items.TryTake(out _, TimeSpan.FromSeconds(1)));
                        }
                    }
                }
                finally
                {
                    foreach (var valueTuple in subscriptions)
                    {
                        valueTuple.Subscription?.Dispose();
                    }
                }
            }
        }

        [Fact]
        public void ShouldOpenSubscriptionWith_WaitForFree_StrategyWhenItIsNotInUseByAnotherClient()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                {
                    Strategy = SubscriptionOpeningStrategy.WaitForFree
                });

                var items = new BlockingCollection<User>();

                subscription.Run(batch => batch.Items.ForEach(x => items.Add(x.Result)));

                using (var s = store.OpenSession())
                {
                    s.Store(new User());
                    s.Store(new User());

                    s.SaveChanges();
                }

                Assert.True(items.TryTake(out _, waitForDocTimeout));
                Assert.True(items.TryTake(out _, waitForDocTimeout));
            }
        }

        [Fact]
        public void ShouldProcessSubscriptionAfterItGetsReleasedWhen_WaitForFree_StrategyIsSet()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();

                var userId = 0;

                foreach (var activeClientStrategy in new[] { SubscriptionOpeningStrategy.OpenIfFree, SubscriptionOpeningStrategy.TakeOver})
                {
                    var activeSubscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                    {
                        Strategy = activeClientStrategy
                    });

                    var pendingSubscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                    {
                        Strategy = SubscriptionOpeningStrategy.WaitForFree
                    });

                    bool batchAcknowledged = false;
                    pendingSubscription.AfterAcknowledgment += x =>
                    {
                        batchAcknowledged = true;
                        return Task.CompletedTask;
                    };

                    var items = new BlockingCollection<User>();

                    pendingSubscription.Run(batch => batch.Items.ForEach(i => items.Add(i.Result)));

                    activeSubscription.Dispose(); // disconnect the active client, the pending one should be notified the the subscription is free and retry to open it

                    using (var s = store.OpenSession())
                    {
                        s.Store(new User(), "users/" + userId++);
                        s.Store(new User(), "users/" + userId++);

                        s.SaveChanges();
                    }

                    User user;

                    Assert.True(items.TryTake(out user, waitForDocTimeout));
                    Assert.Equal("users/" + (userId - 2), user.Id);
                    Assert.True(items.TryTake(out user, waitForDocTimeout));
                    Assert.Equal("users/" + (userId - 1), user.Id);

                    Assert.True(SpinWait.SpinUntil(() => batchAcknowledged, TimeSpan.FromSeconds(5))); // let it acknowledge the processed batch before we open another subscription

                    pendingSubscription.Dispose();
                }
            }
        }

        [Fact]
        public void AllClientsWith_WaitForFree_StrategyShouldGetAccessToSubscription()
        {
            using (var store = GetDocumentStore())
            {
                var id = store.Subscriptions.Create<User>();

                const int numberOfClients = 4;

                var subscriptions = new Subscription<User>[numberOfClients];
                var processed = new bool[numberOfClients];

                int? processedClient = null;

                for (int i = 0; i < numberOfClients; i++)
                {
                    var clientNumber = i;

                    subscriptions[clientNumber] = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(id)
                    {
                        Strategy = SubscriptionOpeningStrategy.WaitForFree
                    });

                    subscriptions[clientNumber].AfterAcknowledgment += x =>
                    {
                        processed[clientNumber] = true;
                        return Task.CompletedTask;
                    };

                    subscriptions[clientNumber].Run(x =>
                    {
                        processedClient = clientNumber;
                    });
                }

                for (int i = 0; i < numberOfClients; i++)
                {
                    using (var s = store.OpenSession())
                    {
                        s.Store(new User());
                        s.SaveChanges();
                    }

                    Assert.True(SpinWait.SpinUntil(() => processedClient != null, waitForDocTimeout));
                    Assert.True(SpinWait.SpinUntil(() => processed[processedClient.Value], waitForDocTimeout));

                    subscriptions[processedClient.Value].Dispose();

                    processedClient = null;
                }

                for (int i = 0; i < numberOfClients; i++)
                {
                    Assert.True(processed[i]);
                }
            }
        }
    }
}
