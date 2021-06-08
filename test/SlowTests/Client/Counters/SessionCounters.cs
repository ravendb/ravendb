using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Counters
{
    public class SessionCounters : RavenTestBase
    {
        public SessionCounters(ITestOutputHelper output) : base(output)
        {
        }

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

                var dic = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes", "downloads" }))
                    .Counters
                    .ToDictionary(c => c.CounterName, c => c.TotalValue);

                Assert.Equal(2, dic.Count);

                Assert.Equal(100, dic["likes"]);
                Assert.Equal(500, dic["downloads"]);

                var val = store.Operations
                    .Send(new GetCountersOperation("users/2-A", new[] { "votes" }))
                    .Counters[0]?.TotalValue;
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

                var counterBatch = new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>()
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "likes",
                                    Delta = 100
                                },
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "downloads",
                                    Delta = 500
                                }
                            }
                        },
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/2-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "votes",
                                    Delta = 1000
                                }
                            }
                        }
                    }
                };

                store.Operations.Send(new CounterBatchOperation(counterBatch));

                var dic = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes", "downloads" }))
                    .Counters
                    .ToDictionary(c => c.CounterName, c => c.TotalValue);

                Assert.Equal(2, dic.Count);
                Assert.Equal(100, dic["likes"]);
                Assert.Equal(500, dic["downloads"]);

                var val = store.Operations
                    .Send(new GetCountersOperation("users/2-A", new[] { "votes" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(1000, val);

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1-A").Delete("likes");
                    session.CountersFor("users/1-A").Delete("downloads");
                    session.CountersFor("users/2-A").Delete("votes");

                    session.SaveChanges();
                }

                var countersDetail = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes", "downloads" }));

                Assert.Equal(2, countersDetail.Counters.Count);
                Assert.Null(countersDetail.Counters[0]);
                Assert.Null(countersDetail.Counters[1]);

                countersDetail = store.Operations
                    .Send(new GetCountersOperation("users/2-A", new[] {"votes"}));
                Assert.Equal(1, countersDetail.Counters.Count);
                Assert.Null(countersDetail.Counters[0]);

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

                var counterBatch = new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>()
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "likes",
                                    Delta = 100
                                },
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "downloads",
                                    Delta = 500
                                }
                            }
                        },
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/2-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "votes",
                                    Delta = 1000
                                }
                            }
                        }
                    }
                };

                store.Operations.Send(new CounterBatchOperation(counterBatch));

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
                    var dic = session.CountersFor("users/1-A").Get(new[] { "likes", "downloads" });

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

                var counterBatch = new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>()
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "likes",
                                    Delta = 100
                                },
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "downloads",
                                    Delta = 500
                                }
                            }
                        },
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/2-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "votes",
                                    Delta = 1000
                                }
                            }
                        }
                    }
                };

                store.Operations.Send(new CounterBatchOperation(counterBatch));

                using (var session = store.OpenAsyncSession())
                {
                    var dic = await session.CountersFor("users/1-A").GetAllAsync();

                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(500, dic["downloads"]);

                    var val = await session.CountersFor("users/2-A").GetAsync("votes");
                    Assert.Equal(1000, val);
                }


                using (var session = store.OpenAsyncSession())
                {
                    var dic = await session.CountersFor("users/1-A").GetAsync(new[] { "likes", "downloads" });

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
                var dbName = GetDatabaseName();
                store.Maintenance.Server.Send(new CreateDatabaseOperation(new DatabaseRecord(dbName)));

                try
                {
                    using (var session = store.OpenSession(dbName))
                    {
                        session.Store(new User { Name = "Aviv1" }, "users/1-A");
                        session.Store(new User { Name = "Aviv2" }, "users/2-A");
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession(dbName))
                    {
                        session.CountersFor("users/1-A").Increment("likes", 100);
                        session.CountersFor("users/1-A").Increment("downloads", 500);
                        session.CountersFor("users/2-A").Increment("votes", 1000);

                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession(dbName))
                    {
                        var dic = session.CountersFor("users/1-A").GetAll();

                        Assert.Equal(2, dic.Count);
                        Assert.Equal(100, dic["likes"]);
                        Assert.Equal(500, dic["downloads"]);

                        var x = session.CountersFor("users/2-A").GetAll();

                        var val = session.CountersFor("users/2-A").Get("votes");
                        Assert.Equal(1000, val);
                    }
                }
                finally
                {
                    store.Maintenance.Server.Send(new DeleteDatabasesOperation(dbName, hardDelete: true));
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
        public void SessionShouldKnowWhenItHasAllCountersInCacheAndAvoidTripToServer_WhenUsingEntity()
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
                    var user = session.Load<User>("users/1-A");
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var userCounters = session.CountersFor(user);

                    var val = userCounters.Get("likes");
                    Assert.Equal(2, session.Advanced.NumberOfRequests);
                    Assert.Equal(100, val);

                    val = userCounters.Get("dislikes");
                    Assert.Equal(3, session.Advanced.NumberOfRequests);
                    Assert.Equal(200, val);

                    val = userCounters.Get("downloads");
                    // session should know at this point that it has all counters

                    Assert.Equal(4, session.Advanced.NumberOfRequests);
                    Assert.Equal(300, val);

                    var dic = userCounters.GetAll(); // should not go to server
                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    Assert.Equal(3, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(300, dic["downloads"]);

                    val = userCounters.Get("score"); // should not go to server
                    Assert.Equal(4, session.Advanced.NumberOfRequests);
                    Assert.Null(val);
                }
            }
        }

        [Fact]
        public void SessionShouldUpdateMissingCountersInCacheAndRemoveDeletedCounters_AfterRefresh()
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
                    var user = session.Load<User>("users/1-A");
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var userCounters = session.CountersFor(user);
                    var dic = userCounters.GetAll();
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.Equal(3, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(300, dic["downloads"]);

                    using (var session2 = store.OpenSession())
                    {
                        session2.CountersFor("users/1-A").Increment("likes");
                        session2.CountersFor("users/1-A").Delete("dislikes");
                        session2.CountersFor("users/1-A").Increment("score", 1000); // new counter
                        session2.SaveChanges();
                    }

                    session.Advanced.Refresh(user);
                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    // Refresh updated the document in session,
                    // cache should know that it's missing 'score' by looking 
                    // at the document's metadata and go to server again to get all.
                    // this should override the cache entirly and therfore 
                    // 'dislikes' won't be in cache anymore

                    dic = userCounters.GetAll();
                    Assert.Equal(4, session.Advanced.NumberOfRequests);

                    Assert.Equal(3, dic.Count);
                    Assert.Equal(101, dic["likes"]);
                    Assert.Equal(300, dic["downloads"]);
                    Assert.Equal(1000, dic["score"]);

                    // cache should know that it got all and not go to server,
                    // and it shouldn't have 'dislikes' entry anymore
                    var val = userCounters.Get("dislikes");
                    Assert.Equal(4, session.Advanced.NumberOfRequests);
                    Assert.Null(val);

                }
            }
        }

        [Fact]
        public void SessionShouldUpdateMissingCountersInCacheAndRemoveDeletedCounters_AfterLoadFromServer()
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
                    var userCounters = session.CountersFor("users/1-A");
                    var dic = userCounters.GetAll();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(3, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(300, dic["downloads"]);

                    using (var session2 = store.OpenSession())
                    {
                        session2.CountersFor("users/1-A").Increment("likes");
                        session2.CountersFor("users/1-A").Delete("dislikes");
                        session2.CountersFor("users/1-A").Increment("score", 1000); // new counter
                        session2.SaveChanges();
                    }

                    session.Load<User>("users/1-A");
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    // now that we have the document in the session,
                    // cache should know that it's missing 'score' by looking at the metadata
                    // and go to server again to get all
                    dic = userCounters.GetAll();
                    Assert.Equal(3, session.Advanced.NumberOfRequests);
                    Assert.Equal(3, dic.Count);

                    Assert.Equal(101, dic["likes"]);
                    Assert.Equal(300, dic["downloads"]);
                    Assert.Equal(1000, dic["score"]);
                }
            }
        }

        [Fact]
        public void SessionClearShouldClearCountersCache()
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
                    var userCounters = session.CountersFor("users/1-A");
                    var dic = userCounters.GetAll();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(3, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(300, dic["downloads"]);

                    using (var session2 = store.OpenSession())
                    {
                        session2.CountersFor("users/1-A").Increment("likes");
                        session2.CountersFor("users/1-A").Delete("dislikes");
                        session2.CountersFor("users/1-A").Increment("score", 1000); // new counter
                        session2.SaveChanges();
                    }

                    session.Advanced.Clear(); // should clear CountersCache

                    dic = userCounters.GetAll(); // should go to server again
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.Equal(3, dic.Count);
                    Assert.Equal(101, dic["likes"]);
                    Assert.Equal(300, dic["downloads"]);
                    Assert.Equal(1000, dic["score"]);
                }
            }
        }

        [Fact]
        public void SessionEvictShouldRemoveEntryFromCountersCache()
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
                    var user = session.Load<User>("users/1-A");
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var userCounters = session.CountersFor("users/1-A");
                    var dic = userCounters.GetAll();
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    Assert.Equal(3, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(300, dic["downloads"]);

                    using (var session2 = store.OpenSession())
                    {
                        session2.CountersFor("users/1-A").Increment("likes");
                        session2.CountersFor("users/1-A").Delete("dislikes");
                        session2.CountersFor("users/1-A").Increment("score", 1000); // new counter
                        session2.SaveChanges();
                    }

                    session.Advanced.Evict(user); // should remove 'users/1-A' entry from  CountersByDocId

                    dic = userCounters.GetAll(); // should go to server again
                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    Assert.Equal(3, dic.Count);
                    Assert.Equal(101, dic["likes"]);
                    Assert.Equal(300, dic["downloads"]);
                    Assert.Equal(1000, dic["score"]);
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

                    var counterBatch = new CounterBatch
                    {
                        Documents = new List<DocumentCountersOperation>()
                        {
                            new DocumentCountersOperation
                            {
                                DocumentId = "users/1-A",
                                Operations = new List<CounterOperation>
                                {
                                    new CounterOperation
                                    {
                                        Type = CounterOperationType.Increment,
                                        CounterName = "likes",
                                        Delta = 400
                                    }
                                }
                            }
                        }
                    };

                    store.Operations.Send(new CounterBatchOperation(counterBatch));

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
        public void SessionIncrementCounterShouldUpdateCounterValueAfterSaveChanges()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("likes", 100);
                    session.CountersFor("users/1-A").Increment("dislikes", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var val = session.CountersFor("users/1-A").Get("likes");
                    Assert.Equal(100, val);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.CountersFor("users/1-A").Increment("likes", 50); // should not increment the counter value in cache
                    val = session.CountersFor("users/1-A").Get("likes");
                    Assert.Equal(100, val);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.CountersFor("users/1-A").Increment("dislikes", 200); // should not add the counter to cache
                    val = session.CountersFor("users/1-A").Get("dislikes");  // should go to server
                    Assert.Equal(300, val);
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    session.CountersFor("users/1-A").Increment("score", 1000); // should not add the counter to cache
                    Assert.Equal(2, session.Advanced.NumberOfRequests);

                    // SaveChanges should updated counters values in cache
                    // according to increment result
                    session.SaveChanges();
                    Assert.Equal(3, session.Advanced.NumberOfRequests);

                    // should not go to server for these
                    val = session.CountersFor("users/1-A").Get("likes");
                    Assert.Equal(150, val);

                    val = session.CountersFor("users/1-A").Get("dislikes");
                    Assert.Equal(500, val);

                    val = session.CountersFor("users/1-A").Get("score");
                    Assert.Equal(1000, val);

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
                    var dic = session.CountersFor("users/1-A").Get(new[] { "likes", "dislikes" });

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

        [Fact]
        public void SessionIncludeSingleCounter()
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
                    var user = session.Load<User>(
                        "users/1-A",
                        i => i.IncludeCounter("likes"));

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var counter = session.CountersFor(user).Get("likes"); // should not go to server

                    Assert.Equal(100, counter);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionChainedIncludeCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");

                    session.CountersFor("orders/1-A").Increment("likes", 100);
                    session.CountersFor("orders/1-A").Increment("dislikes", 200);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(
                        "orders/1-A",
                        i => i.IncludeCounter("likes")
                            .IncludeCounter("dislikes"));

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    var counter = session.CountersFor(order).Get("likes"); // should not go to server                
                    Assert.Equal(100, counter);

                    counter = session.CountersFor(order).Get("dislikes"); // should not go to server
                    Assert.Equal(200, counter);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionChainedIncludeAndIncludeCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Employee { FirstName = "Aviv" }, "employees/1-A");
                    session.Store(new Order { Company = "companies/1-A", Employee = "employees/1-A" }, "orders/1-A");

                    session.CountersFor("orders/1-A").Increment("likes", 100);
                    session.CountersFor("orders/1-A").Increment("dislikes", 200);
                    session.CountersFor("orders/1-A").Increment("downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(
                        "orders/1-A",
                        i => i.IncludeCounter("likes")
                            .IncludeDocuments(o => o.Company)
                            .IncludeCounter("dislikes")
                            .IncludeCounter("downloads")
                            .IncludeDocuments(o => o.Employee));

                    var company = session.Load<Company>(order.Company);
                    Assert.Equal("HR", company.Name);

                    var employee = session.Load<Employee>(order.Employee);
                    Assert.Equal("Aviv", employee.FirstName);

                    var dic = session.CountersFor(order).GetAll(); // should not go to server        
                    Assert.Equal(3, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(300, dic["downloads"]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionIncludeCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");
                    session.CountersFor("orders/1-A").Increment("likes", 100);
                    session.CountersFor("orders/1-A").Increment("dislikes", 200);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(
                        "orders/1-A",
                        i => i.IncludeDocuments("Company")
                            .IncludeCounters(new[] { "likes", "dislikes" }));

                    var company = session.Load<Company>(order.Company);
                    Assert.Equal("HR", company.Name);

                    var dic = session.CountersFor(order).GetAll(); // should not go to server              
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionIncludeAllCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");

                    session.CountersFor("orders/1-A").Increment("likes", 100);
                    session.CountersFor("orders/1-A").Increment("dislikes", 200);
                    session.CountersFor("orders/1-A").Increment("downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(
                        "orders/1-A",
                        i => i.IncludeDocuments("Company")
                            .IncludeAllCounters());

                    var company = session.Load<Company>(order.Company);
                    Assert.Equal("HR", company.Name);

                    var dic = session.CountersFor(order).GetAll(); // should not go to server              
                    Assert.Equal(3, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(300, dic["downloads"]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public async Task AsyncSessionIncludeCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");
                    session.CountersFor("orders/1-A").Increment("likes", 100);
                    session.CountersFor("orders/1-A").Increment("dislikes", 200);

                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(
                        "orders/1-A",
                        i => i.IncludeDocuments("Company")
                            .IncludeCounters(new[] { "likes", "dislikes" }));

                    var company = await session.LoadAsync<Company>(order.Company);
                    Assert.Equal("HR", company.Name);

                    var dic = await session.CountersFor(order).GetAllAsync(); // should not go to server              
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public async Task AsyncSessionIncludeAllCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");

                    session.CountersFor("orders/1-A").Increment("likes", 100);
                    session.CountersFor("orders/1-A").Increment("dislikes", 200);
                    session.CountersFor("orders/1-A").Increment("downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(
                        "orders/1-A",
                        i => i.IncludeDocuments("Company")
                            .IncludeAllCounters());

                    var company = await session.LoadAsync<Company>(order.Company);
                    Assert.Equal("HR", company.Name);

                    var dic = await session.CountersFor(order).GetAllAsync(); // should not go to server              
                    Assert.Equal(3, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(300, dic["downloads"]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

        [Fact]
        public void SessionIncludeSingleCounterAfterIncludeAllCountersShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");

                    session.CountersFor("orders/1-A").Increment("likes", 100);
                    session.CountersFor("orders/1-A").Increment("dislikes", 200);
                    session.CountersFor("orders/1-A").Increment("downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidOperationException>(() =>
                        session.Load<Order>(
                            "orders/1-A",
                            i => i.IncludeDocuments("Company")
                                .IncludeAllCounters()
                                .IncludeCounter("likes")));
                }
            }
        }

        [Fact]
        public void SessionIncludeAllCountersAfterIncludeSingleCounterShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");

                    session.CountersFor("orders/1-A").Increment("likes", 100);
                    session.CountersFor("orders/1-A").Increment("dislikes", 200);
                    session.CountersFor("orders/1-A").Increment("downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidOperationException>(() =>
                        session.Load<Order>(
                            "orders/1-A",
                            i => i.IncludeDocuments("Company")
                                .IncludeCounter("likes")
                                .IncludeAllCounters()));
                }
            }
        }

        [Fact]
        public void SessionIncludeCountersShouldRegisterMissingCounters()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");

                    session.CountersFor("orders/1-A").Increment("likes", 100);
                    session.CountersFor("orders/1-A").Increment("dislikes", 200);
                    session.CountersFor("orders/1-A").Increment("downloads", 300);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(
                        "orders/1-A",
                        i => i.IncludeDocuments("Company")
                            .IncludeCounters(new[] { "likes", "downloads", "dances" })
                            .IncludeCounter("dislikes")
                            .IncludeCounter("cats"));

                    var company = session.Load<Company>(order.Company);
                    Assert.Equal("HR", company.Name);

                    // should not go to server  
                    var dic = session.CountersFor(order).GetAll();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    Assert.Equal(5, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);
                    Assert.Equal(300, dic["downloads"]);

                    //missing counters should be in cache
                    Assert.Null(dic["dances"]);
                    Assert.Null(dic["cats"]);


                }
            }
        }

        [Fact]
        public void SessionIncludeCountersMultipleLoads()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "HR" }, "companies/1-A");
                    session.Store(new Order { Company = "companies/1-A" }, "orders/1-A");

                    session.Store(new Company { Name = "HP" }, "companies/2-A");
                    session.Store(new Order { Company = "companies/2-A" }, "orders/2-A");

                    session.CountersFor("orders/1-A").Increment("likes", 100);
                    session.CountersFor("orders/1-A").Increment("dislikes", 200);

                    session.CountersFor("orders/2-A").Increment("score", 300);
                    session.CountersFor("orders/2-A").Increment("downloads", 400);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var orders = session.Load<Order>(
                        new[] { "orders/1-A", "orders/2-A" },
                        i => i.IncludeDocuments(o => o.Company)
                            .IncludeAllCounters());

                    var order = orders["orders/1-A"];
                    var company = session.Load<Company>(order.Company);
                    Assert.Equal("HR", company.Name);

                    var dic = session.CountersFor(order).GetAll(); // should not go to server              
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(100, dic["likes"]);
                    Assert.Equal(200, dic["dislikes"]);

                    order = orders["orders/2-A"];
                    company = session.Load<Company>(order.Company);
                    Assert.Equal("HP", company.Name);

                    dic = session.CountersFor(order).GetAll(); // should not go to server              
                    Assert.Equal(2, dic.Count);
                    Assert.Equal(300, dic["score"]);
                    Assert.Equal(400, dic["downloads"]);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                }
            }
        }

    }
}
