using System;
using System.Collections.Concurrent;
using System.Threading;
using FastTests.Server.Documents.Notifications;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Documents.Exceptions.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class SubscriptionOperationsSignaling : RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = TimeSpan.FromSeconds(20);// todo: reduce to 20

        [Fact]
        public void WaitOnSubscriptionTaskWhenSubscriptionIsOvertaken()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>());

                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));

                var users = new BlockingCollection<User>();

                subscription.Subscribe(users.Add);
                subscription.Start();

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

                var thread = new Thread(() =>
                {
                    Thread.Sleep(300);
                    concurrentSubscription.Subscribe(users.Add);
                    concurrentSubscription.Start();
                });
                thread.Start();
                Assert.Throws(typeof(AggregateException), () => subscription.SubscriptionLifetimeTask.Wait(_reasonableWaitTime));

                Assert.True(subscription.SubscriptionLifetimeTask.IsFaulted);

                Assert.Equal(typeof(SubscriptionInUseException), subscription.SubscriptionLifetimeTask.Exception.InnerException.GetType());

            }
        }

        [Fact]
        public void SubscriptionInterruptionEventIsFiredWhenSubscriptionIsOvertaken()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));
                var users = new BlockingCollection<User>();

                var mre = new ManualResetEvent(false);
                subscription.SubscriptionConnectionInterrupted += (exception, reconnect) =>
                {
                    if (exception is SubscriptionInUseException && reconnect == false)
                        mre.Set();
                };


                subscription.Subscribe(users.Add);
                subscription.Start();

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User User;
                Assert.True(users.TryTake(out User, _reasonableWaitTime));

                var concurrentSubscription = store.Subscriptions.Open<User>(
                    new SubscriptionConnectionOptions(subscriptionId)
                    {
                        Strategy = SubscriptionOpeningStrategy.TakeOver
                    });

                concurrentSubscription.Subscribe(users.Add);
                concurrentSubscription.Start();
                Assert.True(mre.WaitOne(_reasonableWaitTime));
            }
        }

        [Fact]
        public void WaitOnSubscriptionTaskWhenSubscriptionIsDeleted()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));

                var beforeAckMre = new ManualResetEvent(false);
                var users = new BlockingCollection<User>();
                subscription.Subscribe(users.Add);
                subscription.Start();
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User User;
                Assert.True(users.TryTake(out User, _reasonableWaitTime));


                subscription.BeforeAcknowledgment += () => beforeAckMre.WaitOne();

                store.Subscriptions.Delete(subscriptionId);
                beforeAckMre.Set();

                Assert.Throws(typeof(AggregateException), () => subscription.SubscriptionLifetimeTask.Wait(_reasonableWaitTime));

                Assert.True(subscription.SubscriptionLifetimeTask.IsFaulted);

                Assert.Equal(typeof(SubscriptionDoesNotExistException), subscription.SubscriptionLifetimeTask.Exception.InnerException.GetType());
            }
        }

        [Fact]
        public void SubscriptionInterruptionEventIsFiredWhenSubscriptionIsDeleted()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));
                var users = new BlockingCollection<User>();

                var mre = new ManualResetEvent(false);
                subscription.SubscriptionConnectionInterrupted += (exception, reconnect) =>
                {
                    if (exception is SubscriptionDoesNotExistException && reconnect == false)
                        mre.Set();
                };
                subscription.Subscribe(users.Add);
                subscription.Start();

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User User;
                Assert.True(users.TryTake(out User, _reasonableWaitTime));

                var thread = new Thread(() =>
                {
                    Thread.Sleep(400);
                    store.Subscriptions.Delete(subscriptionId);
                });
                thread.Start();

                Assert.True(mre.WaitOne(_reasonableWaitTime));
            }
        }

        [Fact]
        public void WaitOnSubscriptionTaskWhenSubscriptionCompleted()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));
                var users = new BlockingCollection<User>();
                subscription.Subscribe(users.Add);
                subscription.Start();
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User user;
                Assert.True(users.TryTake(out user, _reasonableWaitTime));

                subscription.Dispose();
                subscription.SubscriptionLifetimeTask.Wait(_reasonableWaitTime);

                Assert.True(subscription.SubscriptionLifetimeTask.IsCompleted);




            }
        }

        [Fact]
        public void WaitOnSubscriptionStopDueToSubscriberError()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));
                var exceptions = new BlockingCollection<Exception>();

                subscription.Subscribe(_ => throw new InvalidCastException(), exceptions.Add);

                subscription.Start();
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                Exception exception;
                Assert.True(exceptions.TryTake(out exception, _reasonableWaitTime));

                Assert.Throws(typeof(AggregateException), () => subscription.SubscriptionLifetimeTask.Wait(_reasonableWaitTime));

                Assert.True(subscription.SubscriptionLifetimeTask.IsFaulted);

                Assert.Equal(typeof(InvalidCastException), subscription.SubscriptionLifetimeTask.Exception.InnerException.GetType());
            }
        }
    }
}
