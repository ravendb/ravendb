using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Notifications;
using Xunit;

namespace FastTests.Client.Subscriptions
{
    public class RavenDB_3491 : RavenTestBase
    {
        private readonly TimeSpan waitForDocTimeout = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(5);

        [Fact]
        public async Task SubscribtionWithEtag()
        {
            using (var store = await GetDocumentStore())
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

                    var user2Etag = session.Advanced.GetEtagFor(us2);
                    var id = await store.AsyncSubscriptions.CreateAsync(new SubscriptionCriteria
                    {
                        Collection = "Users"
                    }, user2Etag ?? 0);

                    var users = new List<RavenJObject>();

                    using (var subscription = store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions
                    {
                        SubscriptionId = id
                    }))
                    {

                        var docs = new BlockingCollection<RavenJObject>();
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        subscription.Subscribe(x => keys.Add(x[Constants.Metadata].Value<string>("@id")));
                        subscription.Subscribe(x => ages.Add(x.Value<int>("Age")));

                        subscription.Subscribe(docs.Add);

                        await subscription.StartAsync();


                        RavenJObject doc;
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        var cnt = users.Count;
                        Assert.Equal(3, cnt);


                        string key;
                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/3", key);

                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/4", key);

                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/5", key);

                        int age;
                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(30, age);

                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(29, age);

                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(34, age);
                    }
                }
            }
        }

        [Fact]
        public async Task StronglyTypedSubscribtionWithStartEtag()
        {
            using (var store = await GetDocumentStore())
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

                    var user2Etag = session.Advanced.GetEtagFor(us2);
                    var id = await store.AsyncSubscriptions.CreateAsync(new SubscriptionCriteria<User>(), user2Etag ?? 0);

                    var users = new List<User>();

                    using (var subscription = store.AsyncSubscriptions.Open<User>(new SubscriptionConnectionOptions
                    {
                        SubscriptionId = id
                    }))
                    {

                        var docs = new BlockingCollection<User>();
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        subscription.Subscribe(x => keys.Add(x.Id));
                        subscription.Subscribe(x => ages.Add(x.Age));

                        subscription.Subscribe(docs.Add);

                        await subscription.StartAsync();

                        User doc;
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        var cnt = users.Count;
                        Assert.Equal(3, cnt);


                        string key;
                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/3", key);

                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/4", key);

                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/5", key);

                        int age;
                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(30, age);

                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(29, age);

                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(34, age);
                    }
                }
            }
        }

        [Fact(Skip = "Racy - need to fix RavenDB-4734")]
        public async Task SubscribtionWithEtag_MultipleOpens()
        {
            using (var store = await GetDocumentStore())
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

                    var user2Etag = session.Advanced.GetEtagFor(us2);
                    subscriptionId = await store.AsyncSubscriptions.CreateAsync(new SubscriptionCriteria
                    {
                        Collection = "Users"
                    }, user2Etag ?? 0);

                    var users = new List<RavenJObject>();

                    using (var subscription = store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions
                    {
                        SubscriptionId = subscriptionId
                    }))
                    {

                        var docs = new BlockingCollection<RavenJObject>();
                        var keys = new BlockingCollection<string>();
                        var ages = new BlockingCollection<int>();

                        subscription.Subscribe(x => keys.Add(x[Constants.Metadata].Value<string>("@id")));
                        subscription.Subscribe(x => ages.Add(x.Value<int>("Age")));

                        subscription.Subscribe(docs.Add);

                        await subscription.StartAsync();

                        RavenJObject doc;
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        Assert.True(docs.TryTake(out doc, waitForDocTimeout));
                        users.Add(doc);
                        var cnt = users.Count;
                        Assert.Equal(3, cnt);


                        string key;
                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/3", key);

                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/4", key);

                        Assert.True(keys.TryTake(out key, waitForDocTimeout));
                        Assert.Equal("users/5", key);

                        int age;
                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(30, age);

                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(29, age);

                        Assert.True(ages.TryTake(out age, waitForDocTimeout));
                        Assert.Equal(34, age);
                    }
                }
                using (var subscription = store.AsyncSubscriptions.Open(new SubscriptionConnectionOptions
                {
                    SubscriptionId = subscriptionId
                }))
                {
                    var docs = new BlockingCollection<RavenJObject>();

                    subscription.Subscribe(o => docs.Add(o));
                    await subscription.StartAsync();

                    RavenJObject item;
                    var tryTake = docs.TryTake(out item, TimeSpan.FromMilliseconds(250));
                    if (tryTake)
                        Console.WriteLine(item.Value<int>("Age"));
                    Assert.False(tryTake);

                }
            }
        }
    }
}
