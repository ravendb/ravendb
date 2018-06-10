using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
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
                    session.CountersFor("users/1-A").Increment("likes", 100);
                    session.CountersFor("users/1-A").Increment("downloads", 500);
                    session.CountersFor("users/2-A").Increment("votes", 1000);

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
                    session.CountersFor("users/1-A").Delete("likes");
                    session.CountersFor("users/1-A").Delete("downloads");
                    session.CountersFor("users/2-A").Delete("votes");

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
                    var dic = session.CountersFor("users/1-A").GetAll();

                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(500, dic["downloads"]);

                    var val = session.CountersFor("users/2-A").Get("votes");
                    Assert.Equal(1000, val);
                }

                using (var session = store.OpenSession())
                {
                    var dic = session.CountersFor("users/1-A").Get(new []{"likes", "downloads"});
                    
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
                    var dic = await session.CountersFor("users/1-A").GetAllAsync();

                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(500, dic["downloads"]);

                    var val = await session.CountersFor("users/2-A").GetAsync( "votes");
                    Assert.Equal(1000, val);
                }


                using (var session = store.OpenAsyncSession())
                {
                    var dic = await session.CountersFor("users/1-A").GetAsync(new []{ "likes", "downloads"});

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
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord("newDatabase")));

                using (var session = store.OpenSession("newDatabase"))
                {
                    session.Store(new User { Name = "Aviv1" }, "users/1-A");
                    session.Store(new User { Name = "Aviv2" }, "users/2-A");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession("newDatabase"))
                {
                    session.CountersFor("users/1-A").Increment("likes", 100);
                    session.CountersFor("users/1-A").Increment("downloads", 500);
                    session.CountersFor("users/2-A").Increment("votes", 1000);

                    session.SaveChanges();
                }


                using (var session = store.OpenSession("newDatabase"))
                {
                    var dic = session.CountersFor("users/1-A").GetAll();

                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(500, dic["downloads"]);

                    var val = session.CountersFor("users/2-A").Get("votes");
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
                    session.CountersFor("users/1-A").Increment("likes", 100);
                    session.CountersFor("users/1-A").Increment("downloads", 100);
                    session.CountersFor("users/2-A").Increment("votes", 1000);

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
                    session.CountersFor("users/1-A").Increment("likes", 100);
                    session.CountersFor("users/1-A").Increment("downloads", 100);
                    session.CountersFor("users/2-A").Increment("votes", 1000);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1-A").Increment("likes", 100);
                    session.CountersFor("users/1-A").Delete("downloads");
                    session.CountersFor("users/2-A").Increment("votes", -600);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.CountersFor("users/1-A").Get("likes");
                    Assert.Equal(200, val);
                    val = session.CountersFor("users/1-A").Get("downloads");
                    Assert.Null(val);
                    val = session.CountersFor("users/2-A").Get("votes");
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
                    session.CountersFor(user).Increment("likes", 100);
                    user.Name += "2";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    Assert.Equal("Aviv2", user.Name);

                    var val = session.CountersFor(user).Get("likes");
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
                    session.CountersFor(user).Increment("likes", 100);
                    session.SaveChanges();

                    Assert.Equal(100, session.CountersFor(user).Get("likes"));
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1-A").Increment("likes", 50);
                    Assert.Throws<InvalidOperationException>(() => session.CountersFor("users/1-A").Delete("likes"));
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1-A").Delete("likes");
                    Assert.Throws<InvalidOperationException>(() => session.CountersFor("users/1-A").Increment("likes", 50));
                }
            }

        }

        [Fact]
        public void SessionShouldTrackCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("likes", 100);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Advanced.NumberOfRequests);

                    var val = session.CountersFor("users/1-A").Get("likes");
                    Assert.Equal(100, val);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.CountersFor("users/1-A").Get("likes");
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionShouldKeepNullsInCountersCache()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("likes", 100);
                    session.CountersFor("users/1-A").Increment("dislikes", 200);
                    session.CountersFor("users/1-A").Increment("downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.CountersFor("users/1-A").Get("score");
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Null(val);

                    val = session.CountersFor("users/1-A").Get("score");
                    //should keep null values in cache
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Null(val);

                    var dic = session.CountersFor("users/1-A").GetAll();
                    //should not contain null value for "score"
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.Equal(3, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(300, dic["downloads"]);
                }
            }
        }

        [Fact]
        public void SessionShouldAlwaysLoadCountersFromCacheAfterGetAll()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("likes", 100);
                    session.CountersFor("users/1-A").Increment("dislikes", 200);
                    session.CountersFor("users/1-A").Increment("downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var dic = session.CountersFor("users/1-A").GetAll();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(3, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(300, dic["downloads"]);

                    //should not go to server after GetAll() request

                    var val = session.CountersFor("users/1-A").Get("likes");
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(100, val);

                    val = session.CountersFor("users/1-A").Get("votes");
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Null(val);

                }
            }
        }

        [Fact]
        public void SessionShouldOverrideExistingCounterValuesInCacheAfterGetAll()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("likes", 100);
                    session.CountersFor("users/1-A").Increment("dislikes", 200);
                    session.CountersFor("users/1-A").Increment("downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.CountersFor("users/1-A").Get("likes");
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(100, val);

                    val = session.CountersFor("users/1-A").Get("score");
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                    Assert.Null(val);

                    store.Counters.Increment("users/1-A", "likes", 400);

                    var dic = session.CountersFor("users/1-A").GetAll();
                    Assert.Equal(3, session.Advanced.NumberOfRequests);
                    Assert.Equal(3, dic.Count); // does not include null value for "score"
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(300, dic["downloads"]);
                    Assert.Equal(500, dic["likes"]); // GetAll() overrides existing values in cache

                    val = session.CountersFor("users/1-A").Get("score"); 
                    Assert.Equal(3, session.Advanced.NumberOfRequests); // null values should still be in cache
                    Assert.Null(val);

                }
            }
        }

        [Fact]
        public void SessionShouldRemoveCounterFromCacheAfterIncrement()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("likes", 100);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.CountersFor("users/1-A").Get("likes");
                    Assert.Equal(100, val);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.CountersFor("users/1-A").Increment("likes", 100);
                    session.SaveChanges();
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    val = session.CountersFor("users/1-A").Get("likes");
                    Assert.Equal(200, val);
                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionShouldRemoveCounterFromCacheAfterCounterDeletion()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("likes", 100);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.CountersFor("users/1-A").Get("likes");
                    Assert.Equal(100, val);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.CountersFor("users/1-A").Delete("likes");
                    session.SaveChanges();
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    val = session.CountersFor("users/1-A").Get("likes");
                    Assert.Null(val);
                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionShouldRemoveCountersFromCacheAfterDocumentDeletion()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("likes", 100);
                    session.CountersFor("users/1-A").Increment("dislikes", 200);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var dic = session.CountersFor("users/1-A").Get(new []{"likes", "dislikes"});

                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.Delete("users/1-A");
                    session.SaveChanges();
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    var val = session.CountersFor("users/1-A").Get("likes");
                    Assert.Equal(3, session.Advanced.NumberOfRequests);
                    Assert.Null(val);
                
                }
            }
        }

    }
}
