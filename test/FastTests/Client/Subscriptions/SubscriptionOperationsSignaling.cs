using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class SubscriptionOperationsSignaling : RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);

        [Fact]
        public void WaitOnSubscriptionTaskWhenSubscriptionIsOvertaken()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionCreationParams = new SubscriptionCreationOptions<User>
                {
                    Criteria = new SubscriptionCriteria<User>()
                };
                var subscriptionId = store.Subscriptions.Create(subscriptionCreationParams);

                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));

                var users = new BlockingCollection<User>();

                var subscirptionLifetimeTask = subscription.Run(u => users.Add(u));

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User user;
                Assert.True(users.TryTake(out user, _reasonableWaitTime));

                var concurrentSubscription = store.Subscriptions.Open<User>(
                    new SubscriptionConnectionOptions(subscriptionId)
                    {
                        Strategy = SubscriptionOpeningStrategy.TakeOver
                    });

                Exception threadException = null;
                var thread = new Thread(() =>
                {
                    try
                    {
                        Thread.Sleep(300);
                        GC.KeepAlive(concurrentSubscription.Run(u => users.Add(u)));
                    }
                    catch (Exception e)
                    {
                        threadException = e;
                    }
                });

                try
                {
                    thread.Start();

                    Assert.Throws(typeof(AggregateException), () => subscirptionLifetimeTask.Wait(_reasonableWaitTime));

                    Assert.True(subscirptionLifetimeTask .IsFaulted);

                    Assert.Equal(typeof(SubscriptionInUseException), subscirptionLifetimeTask.Exception.InnerException.GetType());
                }
                finally
                {
                    thread.Join();
                }

                if (threadException != null)
                    throw threadException;
            }
        }


        [Fact]
        public void WaitOnSubscriptionTaskWhenSubscriptionIsDeleted()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionCreationParams = new SubscriptionCreationOptions<User>
                {
                    Criteria = new SubscriptionCriteria<User>()
                };
                var subscriptionId = store.Subscriptions.Create(subscriptionCreationParams);
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));

                var beforeAckMre = new ManualResetEvent(false);
                var users = new BlockingCollection<User>();
                subscription.BeforeAcknowledgment += () => beforeAckMre.WaitOne();
                var subscriptionLifetimeTask = subscription.Run(u => users.Add(u));
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                Assert.True(users.TryTake(out var _, _reasonableWaitTime));




                store.Subscriptions.Delete(subscriptionId);
                beforeAckMre.Set();

                Assert.Throws(typeof(AggregateException), () => subscriptionLifetimeTask.Wait(_reasonableWaitTime));

                Assert.True(subscriptionLifetimeTask.IsFaulted);

                Assert.Equal(typeof(SubscriptionDoesNotExistException), subscriptionLifetimeTask.Exception.InnerException.GetType());
            }
        }

        [Fact]
        public void WaitOnSubscriptionTaskWhenSubscriptionCompleted()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionCreationParams = new SubscriptionCreationOptions<User>
                {
                    Criteria = new SubscriptionCriteria<User>()
                };
                var subscriptionId = store.Subscriptions.Create(subscriptionCreationParams);
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));
                var users = new BlockingCollection<User>();
                var subscriptionLifetimeTask = subscription.Run(u => users.Add(u));
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User user;
                Assert.True(users.TryTake(out user, _reasonableWaitTime));

                subscription.Dispose();
                subscriptionLifetimeTask.Wait(_reasonableWaitTime);

                Assert.True(subscriptionLifetimeTask.IsCompleted);




            }
        }

        [Fact]
        public void WaitOnSubscriptionStopDueToSubscriberError()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCreationOptions<User>());
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));

                var task = subscription.Run(_ => throw new InvalidCastException());

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var innerException = Assert.Throws<AggregateException>(()=> task .Wait()).InnerException.InnerException;
                Assert.IsType<InvalidCastException>(innerException);
            }
        }
    }
}
