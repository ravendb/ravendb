using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using FastTests.Server.Documents.Notifications;
using Raven.Client.Documents.Subscriptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class RavenDB_3491 : RavenTestBase
    {
        private readonly TimeSpan _waitForDocTimeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(15);

        [Fact]
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
                    var subscriptionCreationParams = new SubscriptionCreationParams
                    {
                        Criteria = new SubscriptionCriteria("Users"),
                        ChangeVector = user2ChangeVector
                    };

                    var id = await store.AsyncSubscriptions.CreateAsync(subscriptionCreationParams);

                    var users = new List<dynamic>();

                    using (var subscription = store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions(id)))
                    {

                        var docs = new BlockingCollection<dynamic>();
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        subscription.Subscribe(x => keys.Add(x.Id));
                        subscription.Subscribe(x => ages.Add(x.Age));

                        subscription.Subscribe(docs.Add);

                        await subscription.StartAsync();


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

        [Fact]
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
                    var subscriptionCreationParams = new SubscriptionCreationParams<User>
                    {
                        Criteria = new SubscriptionCriteria<User>(),
                        ChangeVector = user2ChangeVector
                    };
                    var id = await store.AsyncSubscriptions.CreateAsync(subscriptionCreationParams);

                    var users = new List<User>();

                    using (var subscription = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions(id)))
                    {

                        var docs = new BlockingCollection<User>();
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        subscription.Subscribe(x => keys.Add(x.Id));
                        subscription.Subscribe(x => ages.Add(x.Age));

                        subscription.Subscribe(docs.Add);

                        await subscription.StartAsync();

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

        [Fact]
        public async Task SubscribtionWithEtag_MultipleOpens()
        {
            using (var store = GetDocumentStore())
            {
                var us1 = new User { Id = "users/1", Name = "john", Age = 22 };
                var us2 = new User { Id = "users/2", Name = "KY", Age = 30 };
                var us3 = new User { Id = "users/3", Name = "BEN", Age = 30 };
                var us4 = new User { Id = "users/4", Name = "Hila", Age = 29 };
                var us5 = new User { Id = "users/5", Name = "Revital", Age = 34 };

                long subscriptionId;
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(us1);
                    await session.StoreAsync(us2);
                    await session.StoreAsync(us3);
                    await session.StoreAsync(us4);
                    await session.StoreAsync(us5);
                    await session.SaveChangesAsync();

                    var user2ChangeVector = session.Advanced.GetChangeVectorFor(us2);
                    var subscriptionCreationParams = new SubscriptionCreationParams
                    {
                        Criteria = new SubscriptionCriteria("Users"),
                        ChangeVector = user2ChangeVector
                    };
                    subscriptionId = await store.AsyncSubscriptions.CreateAsync(subscriptionCreationParams);

                    var users = new List<dynamic>();

                    using (var subscription = store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions(subscriptionId)))
                    {
                        var docs = new BlockingCollection<dynamic>();
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        subscription.Subscribe(x => keys.Add(x.Id));
                        subscription.Subscribe(x => ages.Add(x.Age));

                        subscription.Subscribe(docs.Add);

                        await subscription.StartAsync();

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

                using (var subscription = store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions(subscriptionId)))
                {
                    var docs = new BlockingCollection<dynamic>();

                    subscription.Subscribe(o => docs.Add(o));

                    await subscription.StartAsync();

                    dynamic item;
                    var tryTake = docs.TryTake(out item, TimeSpan.FromMilliseconds(250));
                    Assert.False(tryTake);
                }
            }
        }
    }
}
