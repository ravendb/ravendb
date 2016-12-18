// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3484.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Client.Document;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_3484 : RavenTest
    {
        private readonly TimeSpan waitForDocTimeout = TimeSpan.FromSeconds(20);

        [Fact]
        public void OpenIfFree_ShouldBeDefaultStrategy()
        {
            Assert.Equal(SubscriptionOpeningStrategy.OpenIfFree, new SubscriptionConnectionOptions().Strategy);
        }

        [Theory]
        [PropertyData("Storages")]
        public void ShouldRejectWhen_OpenIfFree_StrategyIsUsed(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria());
                store.Subscriptions.Open(id, new SubscriptionConnectionOptions());
                store.Changes().WaitForAllPendingSubscriptions();

                Assert.Throws<SubscriptionInUseException>(() => store.Subscriptions.Open(id, new SubscriptionConnectionOptions()
                {
                    Strategy = SubscriptionOpeningStrategy.OpenIfFree
                }));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void ShouldReplaceActiveClientWhen_TakeOver_StrategyIsUsed(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                
                const int numberOfClients = 4;

                var subscriptions = new Subscription<User>[numberOfClients];
                var items = new BlockingCollection<User>[numberOfClients];

                for (int i = 0; i < numberOfClients; i++)
                {
                    subscriptions[i] = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                    {
                        Strategy = i > 0 ? SubscriptionOpeningStrategy.TakeOver : SubscriptionOpeningStrategy.OpenIfFree
                    });

                    store.Changes().WaitForAllPendingSubscriptions();

                    items[i] = new BlockingCollection<User>();

                    subscriptions[i].Subscribe(items[i].Add);

                    bool batchAcknowledged = false;

                    subscriptions[i].AfterAcknowledgment += x => batchAcknowledged = true;

                    using (var s = store.OpenSession())
                    {
                        s.Store(new User());
                        s.Store(new User());
                        
                        s.SaveChanges();
                    }
                    
                    User user;

                    Assert.True(items[i].TryTake(out user, waitForDocTimeout));
                    Assert.True(items[i].TryTake(out user, waitForDocTimeout));

                    SpinWait.SpinUntil(() => batchAcknowledged, TimeSpan.FromSeconds(5)); // let it acknowledge the processed batch before we open another subscription

                    if (i > 0)
                    {
                        Assert.False(items[i - 1].TryTake(out user, TimeSpan.FromSeconds(2)));
                        Assert.True(SpinWait.SpinUntil(() => subscriptions[i - 1].IsConnectionClosed, TimeSpan.FromSeconds(5)));
                        Assert.True(subscriptions[i - 1].SubscriptionConnectionException is SubscriptionInUseException);
                    }
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void ShouldReplaceActiveClientWhen_ForceAndKeep_StrategyIsUsed(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                foreach (var strategyToReplace in new[] { SubscriptionOpeningStrategy.OpenIfFree, SubscriptionOpeningStrategy.TakeOver, SubscriptionOpeningStrategy.WaitForFree })
                {
                    var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                    var subscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                    {
                        Strategy = strategyToReplace
                    });
                    store.Changes().WaitForAllPendingSubscriptions();
                    var items = new BlockingCollection<User>();

                    subscription.Subscribe(items.Add);

                    var forcedSubscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                    {
                        Strategy = SubscriptionOpeningStrategy.ForceAndKeep
                    });

                    store.Changes().WaitForAllPendingSubscriptions();

                    var forcedItems = new BlockingCollection<User>();

                    forcedSubscription.Subscribe(forcedItems.Add);

                    using (var s = store.OpenSession())
                    {
                        s.Store(new User());
                        s.Store(new User());

                        s.SaveChanges();
                    }

                    User user;

                    Assert.True(forcedItems.TryTake(out user, waitForDocTimeout));
                    Assert.True(forcedItems.TryTake(out user, waitForDocTimeout));

                    Assert.True(SpinWait.SpinUntil(() => subscription.IsConnectionClosed, TimeSpan.FromSeconds(5)));
                    Assert.True(subscription.SubscriptionConnectionException is SubscriptionInUseException);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void OpenIfFree_And_TakeOver_StrategiesCannotDropClientWith_ForceAndKeep_Strategy(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());

                var forcedSubscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                {
                    Strategy = SubscriptionOpeningStrategy.ForceAndKeep
                });

                foreach (var strategy in new[] { SubscriptionOpeningStrategy.OpenIfFree, SubscriptionOpeningStrategy.TakeOver })
                {
                    Assert.Throws<SubscriptionInUseException>(() => store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                    {
                        Strategy = strategy
                    }));
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void ForceAndKeep_StrategyUsageCanTakeOverAnotherClientWith_ForceAndKeep_Strategy(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());

                const int numberOfClients = 4;

                var subscriptions = new Subscription<User>[numberOfClients];
                try
                {
                    var items = new BlockingCollection<User>[numberOfClients];

                    for (int i = 0; i < numberOfClients; i++)
                    {
                        subscriptions[i] = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                        {
                            Strategy = SubscriptionOpeningStrategy.ForceAndKeep
                        });

                        store.Changes().WaitForAllPendingSubscriptions();

                        items[i] = new BlockingCollection<User>();

                        subscriptions[i].Subscribe(items[i].Add);

                        bool batchAcknowledged = false;

                        subscriptions[i].AfterAcknowledgment += x => batchAcknowledged = true;

                        using (var s = store.OpenSession())
                        {
                            s.Store(new User());
                            s.Store(new User());

                            s.SaveChanges();
                        }

                        User user;

                        Assert.True(items[i].TryTake(out user, waitForDocTimeout), $"Waited for {waitForDocTimeout.TotalSeconds} seconds to get notified about a user, giving up... ");
                        Assert.True(items[i].TryTake(out user, waitForDocTimeout), $"Waited for {waitForDocTimeout.TotalSeconds} seconds to get notified about a user, giving up... ");

                        SpinWait.SpinUntil(() => batchAcknowledged, TimeSpan.FromSeconds(5)); // let it acknowledge the processed batch before we open another subscription
                        Assert.True(batchAcknowledged, "Wait for 5 seconds for batch to be acknoeledged, giving up...");
                        if (i > 0)
                        {
                            Assert.False(items[i - 1].TryTake(out user, TimeSpan.FromSeconds(2)),
                                "Was able to take a connection to subscription even though a new connection was opened with ForceAndKeep strategy.");
                            Assert.True(SpinWait.SpinUntil(() => subscriptions[i - 1].IsConnectionClosed, TimeSpan.FromSeconds(5)),
                                "Previous connection to subscription was not closed even though a new connection was opened with ForceAndKeep strategy.");
                            Assert.True(subscriptions[i - 1].SubscriptionConnectionException is SubscriptionInUseException,
                                "SubscriptionConnectionException is not set to expected type, SubscriptionInUseException.");
                        }
                    }
                }
                finally
                {
                    foreach (var subscription in subscriptions)
                    {
                        subscription?.Dispose();
                    }
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void ShouldOpenSubscriptionWith_WaitForFree_StrategyWhenItIsNotInUseByAnotherClient(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                {
                    Strategy = SubscriptionOpeningStrategy.WaitForFree
                });

                var items = new BlockingCollection<User>();

                subscription.Subscribe(items.Add);

                using (var s = store.OpenSession())
                {
                    s.Store(new User());
                    s.Store(new User());

                    s.SaveChanges();
                }

                User user;

                Assert.True(items.TryTake(out user, waitForDocTimeout));
                Assert.True(items.TryTake(out user, waitForDocTimeout));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void ShouldProcessSubscriptionAfterItGetsReleasedWhen_WaitForFree_StrategyIsSet(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());

                var userId = 0;

                foreach (var activeClientStrategy in new []{SubscriptionOpeningStrategy.OpenIfFree, SubscriptionOpeningStrategy.TakeOver, SubscriptionOpeningStrategy.ForceAndKeep})
                {
                    var active = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                    {
                        Strategy = activeClientStrategy
                    });

                    var pending = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                    {
                        Strategy = SubscriptionOpeningStrategy.WaitForFree
                    });

                    Assert.Null(pending.SubscriptionConnectionException);
                    Assert.False(pending.IsConnectionClosed);

                    bool batchAcknowledged = false;
                    pending.AfterAcknowledgment += x => batchAcknowledged = true;

                    var items = new BlockingCollection<User>();

                    pending.Subscribe(items.Add);

                    active.Dispose(); // disconnect the active client, the pending one should be notified the the subscription is free and retry to open it

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

                    pending.Dispose();
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void AllClientsWith_WaitForFree_StrategyShouldGetAccessToSubscription(string storage)
        {
            using (var store = NewDocumentStore(requestedStorage: storage))
            {
                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());

                const int numberOfClients = 4;

                var subscriptions = new Subscription<User>[numberOfClients];
                var processed = new bool[numberOfClients];

                int? processedClient = null;

                for (int i = 0; i < numberOfClients; i++)
                {
                    var clientNumber = i;

                    subscriptions[clientNumber] = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                    {
                        Strategy = SubscriptionOpeningStrategy.WaitForFree
                    });

                    subscriptions[clientNumber].AfterBatch += x =>
                    {
                        processed[clientNumber] = true;
                    };

                    subscriptions[clientNumber].Subscribe(x =>
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
