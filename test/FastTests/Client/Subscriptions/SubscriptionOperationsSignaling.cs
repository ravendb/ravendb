using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Documents.Subscriptions;

namespace FastTests.Client.Subscriptions
{
    public class NamedSubscriptions : RavenTestBase
    {
        [Fact]
        public void CanNameAndOpenWithNameOnly()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionCreationParams = new SubscriptionCreationOptions
                {
                    Name = "get-users",
                };

                var subscriptionId = store.Subscriptions.Create<User>(options: subscriptionCreationParams);

                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions("get-users"));

                var users = new BlockingCollection<User>();

                var subscirptionLifetimeTask = subscription.Run(u =>
                {
                    foreach (var item in u.Items)
                    {
                        users.Add(item.Result);
                    }
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                User user;
                Assert.True(users.TryTake(out user, 1000000));
            }
        }
    }
    public class SubscriptionOperationsSignaling : RavenTestBase
    {
        private readonly TimeSpan _reasonableWaitTime = Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromSeconds(60);

        [Fact]
        public void WaitOnSubscriptionTaskWhenSubscriptionIsOvertaken()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create<User>();

                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));

                var users = new BlockingCollection<User>();

                var subscirptionLifetimeTask = subscription.Run(u =>
                {
                    foreach (var item in u.Items)
                    {
                        users.Add(item.Result);
                    }
                });

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
                        GC.KeepAlive(concurrentSubscription.Run(u =>
                        {
                            foreach (var item in u.Items)
                            {
                                users.Add(item.Result);
                            }
                        }));
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
                
                var subscriptionId = store.Subscriptions.Create<User>();
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));

                var beforeAckMre = new ManualResetEvent(false);
                var users = new BlockingCollection<User>();
                subscription.AfterAcknowledgment += b => { beforeAckMre.WaitOne(); return Task.CompletedTask; };
                var subscriptionLifetimeTask = subscription.Run(u =>
                {
                    foreach (var item in u.Items)
                    {
                        users.Add(item.Result);
                    }
                });
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                Assert.True(users.TryTake(out var _, _reasonableWaitTime));

                store.Subscriptions.DeleteAsync(subscriptionId).Wait();
                beforeAckMre.Set();

                Assert.Throws(typeof(AggregateException), () => subscriptionLifetimeTask.Wait(_reasonableWaitTime));

                Assert.True(subscriptionLifetimeTask.IsFaulted);
            }
        }

        [Fact]
        public void WaitOnSubscriptionTaskWhenSubscriptionCompleted()
        {
            using (var store = GetDocumentStore())
            {
                var subscriptionId = store.Subscriptions.Create<User>();
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));
                var users = new BlockingCollection<User>();
                var subscriptionLifetimeTask = subscription.Run(u =>
                {
                    foreach (var item in u.Items)
                    {
                        users.Add(item.Result);
                    }
                });
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
                var subscriptionId = store.Subscriptions.Create<User>();
                var subscription = store.Subscriptions.Open<User>(new SubscriptionConnectionOptions(subscriptionId));

                var task = subscription.Run(_ => throw new InvalidCastException());

                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var innerException = Assert.Throws<AggregateException>(()=> task .Wait()).InnerException;
                Assert.IsType<SubscriberErrorException>(innerException);
                Assert.IsType<InvalidCastException>(innerException.InnerException);
            }
        }
    }
}
