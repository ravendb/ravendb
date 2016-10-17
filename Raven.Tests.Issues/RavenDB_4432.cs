// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4432.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_4432 : RavenTest
    {
        private readonly TimeSpan waitForDocTimeout = TimeSpan.FromSeconds(20);
        
        public void MultipleTakeOversWorkProperly()
        {
            throw new SkipException("Unreliable");
            using (var store = NewDocumentStore())
            {
                // fill in database to make sure first subscription has something to process
                using (var s = store.OpenSession())
                {
                    s.Store(new User());
                    s.Store(new User());
                    s.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                {
                    Strategy = SubscriptionOpeningStrategy.TakeOver
                });
                store.Changes().WaitForAllPendingSubscriptions();

                var firstSubscriptionStartedProcessingMre = new ManualResetEvent(false);

                subscription.Subscribe(item =>
                {
                    firstSubscriptionStartedProcessingMre.Set();
                    Thread.Sleep(2000);
                });

                // we want to make sure that we open second subscription when first one in processed
                Assert.True(firstSubscriptionStartedProcessingMre.WaitOne(TimeSpan.FromSeconds(10)));

                Action<BlockingCollection<User>> takeOverTask = (BlockingCollection<User> consumer) =>
                {
                    var forcedSubscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                    {
                        Strategy = SubscriptionOpeningStrategy.TakeOver
                    });

                    store.Changes().WaitForAllPendingSubscriptions();

                    using (var s = store.OpenSession())
                    {
                        s.Store(new User());
                        s.SaveChanges();
                    }

                    forcedSubscription.Subscribe(consumer.Add);
                };

                const int threadCount = 5;
                var random = new Random();

                var collections = new BlockingCollection<User>[threadCount];

                for (var i = 0; i < threadCount; i++)
                {
                    var blockingCollection = new BlockingCollection<User>();
                    collections[i] = blockingCollection;
                    Task.Run(() => takeOverTask(blockingCollection));
                    Thread.Sleep(random.Next(100, 1000));
                }

                var joinedCollection = new BlockingCollection<string>();
                User user;

                Parallel.ForEach(collections, collection =>
                {
                    while (collection.TryTake(out user, waitForDocTimeout))
                    {
                        joinedCollection.Add(user.Id);
                    }
                });

                Assert.Equal(threadCount, joinedCollection.Count);
            }
        }

        [Fact]
        public void ShouldHandlePullTimeoutCorrectlyWhenWaitingForAnotherClientToAck()
        {
            using (var store = NewDocumentStore())
            {
                // fill in database to make sure first subscription has something to process
                using (var s = store.OpenSession())
                {
                    s.Store(new User());
                    s.Store(new User());
                    s.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                {
                    Strategy = SubscriptionOpeningStrategy.OpenIfFree
                });
                store.Changes().WaitForAllPendingSubscriptions();
                var items = new BlockingCollection<User>();

                var firstSubscriptionStartedProcessingMre = new ManualResetEvent(false);

                subscription.Subscribe(item =>
                {
                    firstSubscriptionStartedProcessingMre.Set();
                    Thread.Sleep(500);
                    items.Add(item);
                });

                subscription.BeforeAcknowledgment += () =>
                {
                    Thread.Sleep(TimeSpan.FromSeconds(8));
                    return true;
                };

                // we want to make sure that we open second subscription when first one in processed
                Assert.True(firstSubscriptionStartedProcessingMre.WaitOne(TimeSpan.FromSeconds(10)));

                var forcedSubscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                {
                    Strategy = SubscriptionOpeningStrategy.TakeOver,
                    PullingRequestTimeout = TimeSpan.FromSeconds(2)
                });

                store.Changes().WaitForAllPendingSubscriptions();

                var forcedItems = new BlockingCollection<User>();

                forcedSubscription.Subscribe(forcedItems.Add);

                User user;
                // first client should pull and ack all existings docs
                Assert.True(items.TryTake(out user, waitForDocTimeout));
                Assert.True(items.TryTake(out user, waitForDocTimeout));
                Assert.False(items.TryTake(out user, TimeSpan.FromSeconds(1)));

                Assert.False(forcedItems.TryTake(out user, waitForDocTimeout));

                // now fill database with extra records
                using (var s = store.OpenSession())
                {
                    s.Store(new User());
                    s.Store(new User());
                    s.SaveChanges();
                }

                // only second subscription should get results 
                Assert.True(forcedItems.TryTake(out user, waitForDocTimeout));
                Assert.True(forcedItems.TryTake(out user, waitForDocTimeout));
                Assert.False(forcedItems.TryTake(out user, TimeSpan.FromSeconds(1)));

                Assert.False(items.TryTake(out user, TimeSpan.FromSeconds(1)));
            }
        }

        [Theory]
        [InlineData(SubscriptionOpeningStrategy.TakeOver)]
        [InlineData(SubscriptionOpeningStrategy.ForceAndKeep)]
        public void ShouldWaitForClientToProcessCurrentBatchBeforeTakingOver(SubscriptionOpeningStrategy openingStrategy)
        {
            using (var store = NewDocumentStore())
            {
                // fill in database to make sure first subscription has something to process
                using (var s = store.OpenSession())
                {
                    s.Store(new User());
                    s.Store(new User());
                    s.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                {
                    Strategy = SubscriptionOpeningStrategy.OpenIfFree
                });
                store.Changes().WaitForAllPendingSubscriptions();
                var items = new BlockingCollection<User>();

                var firstSubscriptionStartedProcessingMre = new ManualResetEvent(false);

                subscription.Subscribe(item =>
                {
                    firstSubscriptionStartedProcessingMre.Set();
                    Thread.Sleep(500);
                    items.Add(item);
                });

                // we want to make sure that we open second subscription when first one in processed
                Assert.True(firstSubscriptionStartedProcessingMre.WaitOne(TimeSpan.FromSeconds(10)));

                var forcedSubscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                {
                    Strategy = openingStrategy
                });

                store.Changes().WaitForAllPendingSubscriptions();

                var forcedItems = new BlockingCollection<User>();

                forcedSubscription.Subscribe(forcedItems.Add);

                User user;
                // first subscription should contain 2 elements 
                Assert.True(items.TryTake(out user, waitForDocTimeout));
                Assert.True(items.TryTake(out user, waitForDocTimeout));
                Assert.False(items.TryTake(out user, TimeSpan.FromSeconds(1)));

                // forcedItems should be empty at this point as server should wait for first subscription
                // to complete, thus leaving no records to process for second subscription

                Assert.False(forcedItems.TryTake(out user, waitForDocTimeout));
                Assert.True(SpinWait.SpinUntil(() => subscription.IsConnectionClosed, TimeSpan.FromSeconds(5)));
                Assert.True(subscription.SubscriptionConnectionException is SubscriptionInUseException);

                // now fill database with extra records
                using (var s = store.OpenSession())
                {
                    s.Store(new User());
                    s.Store(new User());
                    s.SaveChanges();
                }

                // only second subscription should get results 
                Assert.True(forcedItems.TryTake(out user, waitForDocTimeout));
                Assert.True(forcedItems.TryTake(out user, waitForDocTimeout));
                Assert.False(forcedItems.TryTake(out user, TimeSpan.FromSeconds(1)));

                Assert.False(items.TryTake(out user, TimeSpan.FromSeconds(1)));
            }
        }

        [Theory]
        [InlineData(SubscriptionOpeningStrategy.TakeOver)]
        [InlineData(SubscriptionOpeningStrategy.ForceAndKeep)]
        public void CanTakeOverEvenAfterPreviousSubscriptionTimedOut(SubscriptionOpeningStrategy openingStrategy)
        {
            using (var store = NewDocumentStore())
            {
                // fill in database to make sure first subscription has something to process
                using (var s = store.OpenSession())
                {
                    s.Store(new User());
                    s.Store(new User());
                    s.SaveChanges();
                }

                var id = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions
                {
                    Strategy = SubscriptionOpeningStrategy.OpenIfFree,
                    BatchOptions = new SubscriptionBatchOptions
                    {
                        AcknowledgmentTimeout = TimeSpan.FromSeconds(5)
                    }
                });
                store.Changes().WaitForAllPendingSubscriptions();
                var items = new BlockingCollection<User>();

                var firstSubscriptionStartedProcessingMre = new ManualResetEvent(false);

                subscription.Subscribe(item =>
                {
                    firstSubscriptionStartedProcessingMre.Set();
                    Thread.Sleep(3000); // 2 objects * 3000 gives to 6 second to process, so we can't ack batch
                    items.Add(item);
                });

                // we want to make sure that we open second subscription when first one in processed
                Assert.True(firstSubscriptionStartedProcessingMre.WaitOne(TimeSpan.FromSeconds(10)));

                var forcedSubscription = store.Subscriptions.Open<User>(id, new SubscriptionConnectionOptions()
                {
                    Strategy = openingStrategy
                });

                store.Changes().WaitForAllPendingSubscriptions();

                var forcedItems = new BlockingCollection<User>();

                forcedSubscription.Subscribe(forcedItems.Add);

                User user;
                // first subscription should contain 2 elements - how ever this batch won't be acknowledged
                Assert.True(items.TryTake(out user, waitForDocTimeout));
                Assert.True(items.TryTake(out user, waitForDocTimeout));
                Assert.False(items.TryTake(out user, TimeSpan.FromSeconds(1)));

                // forcedItems should contain also 2 elements because batch in first subscription was not acknowledged on time
                Assert.True(forcedItems.TryTake(out user, waitForDocTimeout));
                Assert.True(forcedItems.TryTake(out user, waitForDocTimeout));
                Assert.False(forcedItems.TryTake(out user, TimeSpan.FromSeconds(1)));

                Assert.True(SpinWait.SpinUntil(() => subscription.IsConnectionClosed, TimeSpan.FromSeconds(5)));
                Assert.True(subscription.SubscriptionConnectionException is SubscriptionInUseException);
            }
        }
    }
}