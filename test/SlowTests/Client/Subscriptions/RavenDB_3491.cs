using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Subscriptions
{
    public class RavenDB_3491 : RavenTestBase
    {
        public RavenDB_3491(ITestOutputHelper output) : base(output)
        {
        }

        private readonly TimeSpan _waitForDocTimeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task SubscribtionWithEtag()
        {
            using (var store = GetDocumentStore())
            {
                var us1 = new User { Id = "users/1", Name = "john", Age = 22 };
                var us2 = new User { Id = "users/2", Name = "KY", Age = 30 };
                var us3 = new User { Id = "users/3", Name = "BEN", Age = 30 };
                var us4 = new User { Id = "users/4", Name = "Hila", Age = 29 };
                var us5 = new User { Id = "users/5", Name = "Revital", Age = 34 };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(us1);
                    await session.StoreAsync(us2);
                    await session.StoreAsync(us3);
                    await session.StoreAsync(us4);
                    await session.StoreAsync(us5);
                    await session.SaveChangesAsync();

                    var user2ChangeVector = session.Advanced.GetChangeVectorFor(us2);
                    var subscriptionCreationParams = new SubscriptionCreationOptions
                    {
                        Query = "from Users",
                        ChangeVector = user2ChangeVector
                    };

                    var id = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                    var users = new List<dynamic>();

                    using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(id)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                    {

                        var docs = new BlockingCollection<dynamic>();
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        GC.KeepAlive(subscription.Run(u =>
                        {
                            foreach (var x in u.Items)
                            {
                                keys.Add(x.Result.Id);
                                ages.Add(x.Result.Age);
                                docs.Add((x.Result));
                            }
                        }));

                        dynamic doc;
                        Assert.True(docs.TryTake(out doc, _waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, _waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, _waitForDocTimeout));
                        users.Add(doc);
                        var cnt = users.Count;
                        Assert.Equal(3, cnt);


                        string key;
                        Assert.True(keys.TryTake(out key, _waitForDocTimeout));
                        Assert.Equal("users/3", key);

                        Assert.True(keys.TryTake(out key, _waitForDocTimeout));
                        Assert.Equal("users/4", key);

                        Assert.True(keys.TryTake(out key, _waitForDocTimeout));
                        Assert.Equal("users/5", key);

                        int age;
                        Assert.True(ages.TryTake(out age, _waitForDocTimeout));
                        Assert.Equal(30, age);

                        Assert.True(ages.TryTake(out age, _waitForDocTimeout));
                        Assert.Equal(29, age);

                        Assert.True(ages.TryTake(out age, _waitForDocTimeout));
                        Assert.Equal(34, age);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task StronglyTypedSubscribtionWithStartEtag()
        {
            using (var store = GetDocumentStore())
            {
                var us1 = new User { Id = "users/1", Name = "john", Age = 22 };
                var us2 = new User { Id = "users/2", Name = "KY", Age = 30 };
                var us3 = new User { Id = "users/3", Name = "BEN", Age = 30 };
                var us4 = new User { Id = "users/4", Name = "Hila", Age = 29 };
                var us5 = new User { Id = "users/5", Name = "Revital", Age = 34 };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(us1);
                    await session.StoreAsync(us2);
                    await session.StoreAsync(us3);
                    await session.StoreAsync(us4);
                    await session.StoreAsync(us5);
                    await session.SaveChangesAsync();

                    var user2ChangeVector = session.Advanced.GetChangeVectorFor(us2);
                    var subscriptionCreationParams = new SubscriptionCreationOptions
                    {
                        ChangeVector = user2ChangeVector
                    };
                    var id = await store.Subscriptions.CreateAsync<User>(options: subscriptionCreationParams);

                    var users = new List<User>();

                    using (var subscription = store.Subscriptions.GetSubscriptionWorker<User>(new SubscriptionWorkerOptions(id)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                    }))
                    {

                        var docs = new BlockingCollection<User>();
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        GC.KeepAlive(subscription.Run(u =>
                        {
                            foreach (var x in u.Items)
                            {
                                keys.Add(x.Result.Id);
                                ages.Add(x.Result.Age);
                                docs.Add((x.Result));
                            }
                        }));


                        User doc;
                        Assert.True(docs.TryTake(out doc, _waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, _waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, _waitForDocTimeout));
                        users.Add(doc);
                        var cnt = users.Count;
                        Assert.Equal(3, cnt);


                        string key;
                        Assert.True(keys.TryTake(out key, _waitForDocTimeout));
                        Assert.Equal("users/3", key);

                        Assert.True(keys.TryTake(out key, _waitForDocTimeout));
                        Assert.Equal("users/4", key);

                        Assert.True(keys.TryTake(out key, _waitForDocTimeout));
                        Assert.Equal("users/5", key);

                        int age;
                        Assert.True(ages.TryTake(out age, _waitForDocTimeout));
                        Assert.Equal(30, age);

                        Assert.True(ages.TryTake(out age, _waitForDocTimeout));
                        Assert.Equal(29, age);

                        Assert.True(ages.TryTake(out age, _waitForDocTimeout));
                        Assert.Equal(34, age);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Subscriptions)]
        public async Task SubscribtionWithEtag_MultipleOpens()
        {
            using (var store = GetDocumentStore())
            {
                var us1 = new User { Id = "users/1", Name = "john", Age = 22 };
                var us2 = new User { Id = "users/2", Name = "KY", Age = 30 };
                var us3 = new User { Id = "users/3", Name = "BEN", Age = 30 };
                var us4 = new User { Id = "users/4", Name = "Hila", Age = 29 };
                var us5 = new User { Id = "users/5", Name = "Revital", Age = 34 };

                string subscriptionName;
                var subscriptionReleasedAwaiter = Task.CompletedTask;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(us1);
                    await session.StoreAsync(us2);
                    await session.StoreAsync(us3);
                    await session.StoreAsync(us4);
                    await session.StoreAsync(us5);
                    await session.SaveChangesAsync();

                    var user2ChangeVector = session.Advanced.GetChangeVectorFor(us2);
                    var subscriptionCreationParams = new SubscriptionCreationOptions
                    {
                        Query = "from Users",
                        ChangeVector = user2ChangeVector
                    };
                    subscriptionName = await store.Subscriptions.CreateAsync(subscriptionCreationParams);

                    var users = new List<dynamic>();

                    using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subscriptionName)
                    {
                        TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)

                    }))
                    {
                        var docs = new BlockingCollection<dynamic>();
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        var mre = new AsyncManualResetEvent();
                        subscription.AfterAcknowledgment += batch =>
                        {
                            mre.Set();
                            return Task.CompletedTask;
                        };

                        GC.KeepAlive(subscription.Run(u =>
                        {
                            foreach (var x in u.Items)
                            {
                                keys.Add(x.Result.Id);
                                ages.Add(x.Result.Age);
                                docs.Add((x.Result));
                            }
                        }));
                        var db = await GetDatabase(store.Database);

                        var subscriptionState = await AssertWaitForNotNullAsync(() =>
                        {
                            using (Server.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                            using (ctx.OpenReadTransaction())
                            {
                                return Task.FromResult(db.SubscriptionStorage.GetSubscriptionConnectionsState(ctx, subscriptionName));
                            }
                        });

                        subscriptionReleasedAwaiter = subscriptionState.GetSubscriptionInUseAwaiter;

                        dynamic doc;
                        Assert.True(docs.TryTake(out doc, _waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, _waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, _waitForDocTimeout));
                        users.Add(doc);
                        var cnt = users.Count;
                        Assert.Equal(3, cnt);

                        string key;
                        Assert.True(keys.TryTake(out key, _waitForDocTimeout));
                        Assert.Equal("users/3", key);

                        Assert.True(keys.TryTake(out key, _waitForDocTimeout));
                        Assert.Equal("users/4", key);

                        Assert.True(keys.TryTake(out key, _waitForDocTimeout));
                        Assert.Equal("users/5", key);

                        int age;
                        Assert.True(ages.TryTake(out age, _waitForDocTimeout));
                        Assert.Equal(30, age);

                        Assert.True(ages.TryTake(out age, _waitForDocTimeout));
                        Assert.Equal(29, age);

                        Assert.True(ages.TryTake(out age, _waitForDocTimeout));
                        Assert.Equal(34, age);

                        Assert.True(await mre.WaitAsync(TimeSpan.FromMilliseconds(250)));
                    }
                }

                Assert.True(Task.WaitAll(new[] { subscriptionReleasedAwaiter }, 250));

                using (var subscription = store.Subscriptions.GetSubscriptionWorker(new SubscriptionWorkerOptions(subscriptionName)
                {
                    TimeToWaitBeforeConnectionRetry = TimeSpan.FromSeconds(5)
                }))
                {
                    var docs = new BlockingCollection<dynamic>();
                    GC.KeepAlive(subscription.Run(x =>
                    {
                        docs.Add((x));
                    }));

                    dynamic item;
                    var tryTake = docs.TryTake(out item, TimeSpan.FromMilliseconds(250));
                    Assert.False(tryTake);
                }
            }
        }
    }
}
