using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions.Subscriptions;
using Raven.Client.Document;
using Raven.Tests.Helpers;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Subscriptions
{
    public class SubscriptionOperationsSignaling:RavenTestBase
    {
        private TimeSpan reasonableWaitTime = TimeSpan.FromSeconds(20);

        [Fact]
        public void WaitOnSubscriptionTaskWhenSubscriptionIsOvertaken()
        {
            using (var store = NewDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(subscriptionId, new SubscriptionConnectionOptions()
                {
                    ClientAliveNotificationInterval = TimeSpan.FromSeconds(1),
                    BatchOptions = new SubscriptionBatchOptions
                    {
                        
                    }
                });
                var users = new BlockingCollection<User>();

                subscription.Subscribe(users.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User User;
                Assert.True(users.TryTake(out User, reasonableWaitTime));

                  var concurrentSubscription = store.Subscriptions.Open<User>(subscriptionId, new SubscriptionConnectionOptions()
                {
                      ClientAliveNotificationInterval = TimeSpan.FromSeconds(1),
                      Strategy = SubscriptionOpeningStrategy.TakeOver
                });

                var thread = new Thread(() => {
                    Thread.Sleep(300);
                    concurrentSubscription.Subscribe(users.Add); });
                thread.Start();
                Assert.Throws(typeof(AggregateException), ()=>subscription.SubscriptionLifetimeTask.Wait(reasonableWaitTime));

                Assert.True(subscription.SubscriptionLifetimeTask.IsFaulted);

                Assert.Equal(typeof(SubscriptionInUseException), subscription.SubscriptionLifetimeTask.Exception.InnerException.GetType());
            
            }
        }

        [Fact]
        public void SubscriptionInterruptionEventIsFiredWhenSubscriptionIsOvertaken()
        {
            using (var store = NewDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(subscriptionId, new SubscriptionConnectionOptions()
                {
                    ClientAliveNotificationInterval = TimeSpan.FromSeconds(1),
                    BatchOptions = new SubscriptionBatchOptions
                    {

                    }
                });
                var users = new BlockingCollection<User>();

                var mre = new ManualResetEvent(false);
                subscription.SubscriptionConnectionInterrupted += (exception, reconnect) =>
                {
                    if (exception is SubscriptionInUseException && reconnect == false)
                        mre.Set();
                };


                subscription.Subscribe(users.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User User;
                Assert.True(users.TryTake(out User, reasonableWaitTime));

                var concurrentSubscription = store.Subscriptions.Open<User>(subscriptionId, new SubscriptionConnectionOptions()
                {
                    ClientAliveNotificationInterval = TimeSpan.FromSeconds(1),
                    Strategy = SubscriptionOpeningStrategy.TakeOver
                });

                concurrentSubscription.Subscribe(users.Add);

                Assert.True(mre.WaitOne(reasonableWaitTime));
            }
        }

        [Fact]
        public void WaitOnSubscriptionTaskWhenSubscriptionIsDeleted()
        {
            using (var store = NewDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(subscriptionId, new SubscriptionConnectionOptions()
                {
                    ClientAliveNotificationInterval = TimeSpan.FromSeconds(1),
                    BatchOptions = new SubscriptionBatchOptions
                    {
                        
                    }
                });

                var beforeAckMre = new ManualResetEvent(false);
                var users = new BlockingCollection<User>();
                subscription.Subscribe(users.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User User;
                Assert.True(users.TryTake(out User, reasonableWaitTime));


                subscription.BeforeAcknowledgment += () => beforeAckMre.WaitOne();

                store.Subscriptions.Delete(subscriptionId);
                beforeAckMre.Set();

                Assert.Throws(typeof(AggregateException), () => subscription.SubscriptionLifetimeTask.Wait(reasonableWaitTime));

                Assert.True(subscription.SubscriptionLifetimeTask.IsFaulted);

                Assert.Equal(typeof(SubscriptionDoesNotExistException), subscription.SubscriptionLifetimeTask.Exception.InnerException.GetType());
            }
        }

        [Fact]
        public void SubscriptionInterruptionEventIsFiredWhenSubscriptionIsDeleted()
        {
            using (var store = NewDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(subscriptionId, new SubscriptionConnectionOptions()
                {
                    ClientAliveNotificationInterval = TimeSpan.FromSeconds(1),
                    BatchOptions = new SubscriptionBatchOptions
                    {
                        
                    }
                });
                var users = new BlockingCollection<User>();

                var mre = new ManualResetEvent(false);
                subscription.SubscriptionConnectionInterrupted += (exception, reconnect) =>
                {
                    if (exception is SubscriptionDoesNotExistException && reconnect == false)
                        mre.Set();
                };
                subscription.Subscribe(users.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User User;
                Assert.True(users.TryTake(out User, reasonableWaitTime));

                var thread = new Thread(() =>
                {
                    Thread.Sleep(400);
                    store.Subscriptions.Delete(subscriptionId);
                });
                thread.Start();
                Assert.True(mre.WaitOne(reasonableWaitTime));
            }
        }

        [Fact]
        public void WaitOnSubscriptionTaskWhenSubscriptionCompleted()
        {
            using (var store = NewDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>()
                {
                    
                });
                var subscription = store.Subscriptions.Open<User>(subscriptionId, new SubscriptionConnectionOptions()
                {
                    ClientAliveNotificationInterval = TimeSpan.FromSeconds(1),
                    BatchOptions = new SubscriptionBatchOptions
                    {
                        
                    }
                });
                var users = new BlockingCollection<User>();
                subscription.Subscribe(users.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User User;
                Assert.True(users.TryTake(out User, reasonableWaitTime));

                subscription.Dispose();

                Assert.DoesNotThrow(() => subscription.SubscriptionLifetimeTask.Wait(reasonableWaitTime));

                Assert.True(subscription.SubscriptionLifetimeTask.IsCompleted);


              

            }
        }

        [Fact]
        public void TrackSubscriptionResartDueToAckTimespan()
        {
            using (var store = NewDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>()
                {

                });
                var subscription = store.Subscriptions.Open<User>(subscriptionId, new SubscriptionConnectionOptions()
                {
                    ClientAliveNotificationInterval = TimeSpan.FromSeconds(1),
                    BatchOptions = new SubscriptionBatchOptions
                    {
                        AcknowledgmentTimeout = TimeSpan.FromDays(-1)
                    }
                });
                var users = new BlockingCollection<User>();
                var mre = new ManualResetEvent(false);
                subscription.SubscriptionConnectionInterrupted += (exception, reconnect) =>
                {
                    if (reconnect == true && exception is SubscriptionAckTimeoutException)
                        mre.Set();

                };
                subscription.Subscribe(users.Add);

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User User;

                Assert.True(users.TryTake(out User, reasonableWaitTime));
                Assert.True(mre.WaitOne(reasonableWaitTime));
            }
        }

        [Fact]
        public void WaitOnSubscriptionStopDueToSubscriberError()
        {
            using (var store = NewDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create(new SubscriptionCriteria<User>());
                var subscription = store.Subscriptions.Open<User>(subscriptionId, new SubscriptionConnectionOptions()
                {
                    ClientAliveNotificationInterval = TimeSpan.FromSeconds(1),
                    BatchOptions = new SubscriptionBatchOptions
                    {

                    }
                });
                var exceptions = new BlockingCollection<Exception>();
                subscription.Subscribe(_ =>
                {
                    throw new InvalidCastException();
                }, exceptions.Add
                );

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                Exception exception;
                Assert.True(exceptions.TryTake(out exception, reasonableWaitTime));
                
                Assert.Throws(typeof(AggregateException), () => subscription.SubscriptionLifetimeTask.Wait(reasonableWaitTime));

                Assert.True(subscription.SubscriptionLifetimeTask.IsFaulted);

                Assert.Equal(typeof(InvalidCastException), subscription.SubscriptionLifetimeTask.Exception.InnerException.GetType());
            }
        }
    }
}
