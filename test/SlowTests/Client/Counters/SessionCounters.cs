using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class SessionCounters : RavenTestBase
    {
        [Fact]
        public void SessionIncrementCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv1" }, "users/1-A");
                    session.Store(new User { Name = "Aviv2" }, "users/2-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Counters.Increment("users/1-A", "likes", 100);
                    session.Advanced.Counters.Increment("users/1-A", "downloads", 500);
                    session.Advanced.Counters.Increment("users/2-A", "votes", 1000);

                    session.SaveChanges();
                }

                var dic = store.Counters.Get("users/1-A", new[] {"likes", "downloads"});
                Assert.Equal(2, dic.Count);

                Assert.Equal(100, dic["likes"]);
                Assert.Equal(500, dic["downloads"]);

                var val = store.Counters.Get("users/2-A", "votes");
                Assert.Equal(1000, val);
            }
        }

        [Fact]
        public void SessionDeleteCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv1" }, "users/1-A");
                    session.Store(new User { Name = "Aviv2" }, "users/2-A");
                    session.SaveChanges();
                }

                store.Counters.Increment("users/1-A", "likes", 100);
                store.Counters.Increment("users/1-A", "downloads", 500);
                store.Counters.Increment("users/2-A", "votes", 1000);

                var dic = store.Counters.Get("users/1-A", new[] {"likes", "downloads"});

                Assert.Equal(2, dic.Count);
                Assert.Equal(100, dic["likes"]);
                Assert.Equal(500, dic["downloads"]);

                var val = store.Counters.Get("users/2-A", "votes");
                Assert.Equal(1000, val);

                using (var session = store.OpenSession())
                {
                    session.Advanced.Counters.Delete("users/1-A", "likes");
                    session.Advanced.Counters.Delete("users/1-A", "downloads");
                    session.Advanced.Counters.Delete("users/2-A", "votes");

                    session.SaveChanges();
                }

                dic = store.Counters.Get("users/1-A", new[] { "likes", "downloads" });
                Assert.Equal(0, dic.Count);

                val = store.Counters.Get("users/2-A", "votes");
                Assert.Null(val);

            }
        }

        [Fact]
        public void SessionGetCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv1" }, "users/1-A");
                    session.Store(new User { Name = "Aviv2" }, "users/2-A");
                    session.SaveChanges();
                }

                store.Counters.Increment("users/1-A", "likes", 100);
                store.Counters.Increment("users/1-A", "downloads", 500);
                store.Counters.Increment("users/2-A", "votes", 1000);

                using (var session = store.OpenSession())
                {
                    var dic = session.Advanced.Counters.Get("users/1-A");

                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(500, dic["downloads"]);

                    var val = session.Advanced.Counters.Get("users/2-A", "votes");
                    Assert.Equal(1000, val);
                }

                using (var session = store.OpenSession())
                {
                    //test with Get(string docId, params string[] counters) overload
                    var dic = session.Advanced.Counters.Get("users/1-A", "likes", "downloads");

                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(500, dic["downloads"]);
                }
            }
        }

        [Fact]
        public async Task SessionGetCountersAsync()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv1" }, "users/1-A");
                    await session.StoreAsync(new User { Name = "Aviv2" }, "users/2-A");
                    await session.SaveChangesAsync();
                }

                await store.Counters.IncrementAsync("users/1-A", "likes", 100);
                await store.Counters.IncrementAsync("users/1-A", "downloads", 500);
                await store.Counters.IncrementAsync("users/2-A", "votes", 1000);

                using (var session = store.OpenAsyncSession())
                {
                    var dic = await session.Advanced.Counters.GetAsync("users/1-A");

                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(500, dic["downloads"]);

                    var val = await session.Advanced.Counters.GetAsync("users/2-A", "votes");
                    Assert.Equal(1000, val);
                }

                using (var session = store.OpenAsyncSession())
                {
                    //test with GetAsync(string docId, params string[] counters) overload
                    var dic = await session.Advanced.Counters.GetAsync("users/1-A", "likes", "downloads");

                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(500, dic["downloads"]);
                }
            }
        }

        [Fact]
        public void SessionGetCountersWithNonDefaultDatabase()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord("newDatabase")
                {
                    Settings =
                    {
                        [RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)] = "Experimental"
                    }
                }));

                using (var session = store.OpenSession("newDatabase"))
                {
                    session.Store(new User { Name = "Aviv1" }, "users/1-A");
                    session.Store(new User { Name = "Aviv2" }, "users/2-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession("newDatabase"))
                {   
                    session.Advanced.Counters.Increment("users/1-A", "likes", 100);
                    session.Advanced.Counters.Increment("users/1-A", "downloads", 500);
                    session.Advanced.Counters.Increment("users/2-A", "votes", 1000);

                    session.SaveChanges();
                }


                using (var session = store.OpenSession("newDatabase"))
                {
                    var dic = session.Advanced.Counters.Get("users/1-A");

                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(500, dic["downloads"]);

                    var val = session.Advanced.Counters.Get("users/2-A", "votes");
                    Assert.Equal(1000, val);
                }
            }
        }

        [Fact]
        public void GetCountersFor()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv1" }, "users/1-A");
                    session.Store(new User { Name = "Aviv2" }, "users/2-A");
                    session.Store(new User { Name = "Aviv3" }, "users/3-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Counters.Increment("users/1-A", "likes", 100);
                    session.Advanced.Counters.Increment("users/1-A", "downloads", 500);
                    session.Advanced.Counters.Increment("users/2-A", "votes", 1000);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var counters = session.Advanced.GetCountersFor(user);

                    Assert.Equal(2, counters.Count);
                    Assert.Equal("downloads", counters[0]);
                    Assert.Equal("likes", counters[1]);

                    user = session.Load<User>("users/2-A");
                    counters = session.Advanced.GetCountersFor(user);

                    Assert.Equal(1, counters.Count);
                    Assert.Equal("votes", counters[0]);

                    user = session.Load<User>("users/3-A");
                    counters = session.Advanced.GetCountersFor(user);
                    Assert.Null(counters);
                }
            }

        }

        [Fact]
        public void DifferentTypesOfCountersOperationsInOneSession()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv1" }, "users/1-A");
                    session.Store(new User { Name = "Aviv2" }, "users/2-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Counters.Increment("users/1-A", "likes", 100);
                    session.Advanced.Counters.Increment("users/1-A", "downloads", 500);
                    session.Advanced.Counters.Increment("users/2-A", "votes", 1000);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Counters.Increment("users/1-A", "likes", 100);
                    session.Advanced.Counters.Delete("users/1-A", "downloads");
                    session.Advanced.Counters.Increment("users/2-A", "votes", -600);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.Advanced.Counters.Get("users/1-A", "likes");
                    Assert.Equal(200, val);
                    val = session.Advanced.Counters.Get("users/1-A", "downloads");
                    Assert.Null(val);
                    val = session.Advanced.Counters.Get("users/2-A", "votes");
                    Assert.Equal(400, val);
                }
            }
        }

        [Fact]
        public void IncrementCounterAndModifyDocInOneSession()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    session.Advanced.Counters.Increment(user, "likes", 100);
                    user.Name += "2";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    Assert.Equal("Aviv2", user.Name);

                    var val = session.Advanced.Counters.Get(user, "likes");
                    Assert.Equal(100, val);
                }
            }
        }

        [Fact]
        public void ShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    session.Advanced.Counters.Increment(user, "likes", 100);
                    session.SaveChanges();

                    Assert.Equal(100, session.Advanced.Counters.Get(user, "likes"));
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Counters.Increment("users/1-A", "likes", 50);
                    Assert.Throws<InvalidOperationException>(() => session.Advanced.Counters.Delete("users/1-A", "likes"));
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Counters.Delete("users/1-A", "likes");
                    Assert.Throws<InvalidOperationException>(() => session.Advanced.Counters.Increment("users/1-A", "likes", 50));
                }
            }

        }
    }
}
