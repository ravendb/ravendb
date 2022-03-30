using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Queries;
using Xunit;
using PatchRequest = Raven.Client.Documents.Operations.PatchRequest;
using Xunit.Abstractions;

namespace SlowTests.Client.Counters
{
    public class PatchOnCounters : RavenTestBase
    {
        public PatchOnCounters(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanIncrementSingleCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Aviv"
                    }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                store.Operations.Send(new PatchOperation("users/1-A", null, new PatchRequest
                {
                    Script = "incrementCounter(this, args.name, args.val)",
                    Values =
                    {
                        { "name", "Downloads" },
                        { "val", 100 }
                    }
                }));

                var val = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] {"Downloads"}))
                    .Counters[0]?.TotalValue;

                Assert.Equal(200, val);
            }
        }

        [Fact]
        public void CanIncrementSingleCounterWithId()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Aviv"
                    }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                store.Operations.Send(new PatchOperation("users/1-A", null, new PatchRequest
                {
                    Script = "incrementCounter(id(this), args.name, args.val)",
                    Values =
                    {
                        { "name", "Downloads" },
                        { "val", 100 }
                    }
                }));

                var val = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "Downloads" }))
                    .Counters[0]?.TotalValue;

                Assert.Equal(200, val);
            }
        }

        [Fact]
        public void AddingNewCounterShouldUpdateMetadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Aviv"
                    }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                store.Operations.Send(new PatchOperation("users/1-A", null, new PatchRequest
                {
                    Script = "incrementCounter(this, args.name, args.val)",
                    Values =
                    {
                        { "name", "Likes" },
                        { "val", 200 }
                    }
                }));

                var val = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "Likes" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(200, val);

                using (var session = store.OpenSession())
                {
                    var u = session.Load<User>("users/1-A");
                    var counters = session.Advanced.GetCountersFor(u);
                    Assert.Equal(2, counters.Count);
                    Assert.Equal("Downloads", counters[0]);
                    Assert.Equal("Likes", counters[1]);
                }
            }
        }

        [Fact]
        public void CounterDeletionShouldUpdateMetadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Aviv"
                    }, "users/1-A");

                    var countersFor = session.CountersFor("users/1-A");
                    countersFor.Increment("Downloads", 100);
                    countersFor.Increment("Likes", 200);

                    session.SaveChanges();
                }

                store.Operations.Send(new PatchOperation("users/1-A", null, new PatchRequest
                {
                    Script = "deleteCounter(this, args.name)",
                    Values =
                    {
                        { "name", "Downloads" }
                    }
                }));

                using (var session = store.OpenSession())
                {
                    var countersFor = session.CountersFor("users/1-A");
                    var val = countersFor.Get("Likes");
                    Assert.Equal(200, val);

                    val = countersFor.Get("Downloads");
                    Assert.Null(val);
                }

                using (var session = store.OpenSession())
                {
                    var u = session.Load<User>("users/1-A");
                    var counters = session.Advanced.GetCountersFor(u);

                    Assert.Equal(1, counters.Count);
                    Assert.Equal("Likes", counters[0]);
                }
            }
        }

        [Fact]
        public void CanGetCounterNameFromMetadataAndIncrement()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Aviv"
                    }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                store.Operations.Send(new PatchOperation("users/1-A", null, new PatchRequest
                {
                    Script = @"{
                                    var name = this[""@metadata""][""@counters""][0];
                                    incrementCounter(this, name, args.val);
                               }",
                    Values =
                    {
                        { "val", 100 }
                    }
                }));

                var val = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "Downloads" }))
                    .Counters[0]?.TotalValue;

                Assert.Equal(200, val);
            }
        }

        [Fact]
        public void CanIncrementTwoCountersAtOnce()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Aviv",
                        Friend = "users/2-A"
                    }, "users/1-A");
                    session.Store(new User
                    {
                        Name = "Gonras",
                        Friend = "users/1-A"
                    }, "users/2-A");

                    session.CountersFor("users/1-A").Increment("Score", 100);
                    session.CountersFor("users/2-A").Increment("Score", 300);
                    session.SaveChanges();
                }

                store.Operations.Send(new PatchOperation("users/1-A", null, new PatchRequest
                {
                    Script = @"{ 
                                    incrementCounter(this, args.name, args.val);
                                    incrementCounter(this.Friend, args.name, args.val * -1);
                               }",
                    Values =
                    {
                        { "name", "Score" },
                        { "val", 50 }
                    }
                }));

                var val = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "Score" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(150, val);

                val = store.Operations
                    .Send(new GetCountersOperation("users/2-A", new[] { "Score" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(250, val);
            }
        }

        [Fact]
        public void CanIncrementCountersViaPatchByQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "George"
                    }, "users/1-A");
                    session.Store(new User
                    {
                        Name = "John"
                    }, "users/2-A");
                    session.Store(new User
                    {
                        Name = "Paul"
                    }, "users/3-A");
                    session.Store(new User
                    {
                        Name = "Ringo"
                    }, "users/4-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Downloads", 400);
                    session.CountersFor("users/4-A").Increment("Downloads", 800);

                    session.SaveChanges();
                }

                store.Operations
                     .Send(new PatchByQueryOperation(new IndexQuery
                     {
                         Query = @"from Users as u
                                  update
                                  {
                                      incrementCounter(u, ""Downloads"", 100)
                                  }"
                     })).WaitForCompletion(TimeSpan.FromMinutes(5));

                var val = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "Downloads" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(200, val);

                val = store.Operations
                    .Send(new GetCountersOperation("users/2-A", new[] { "Downloads" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(300, val);

                val = store.Operations
                    .Send(new GetCountersOperation("users/3-A", new[] { "Downloads" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(500, val);

                val = store.Operations
                    .Send(new GetCountersOperation("users/4-A", new[] { "Downloads" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(900, val);
            }
        }

        [Fact]
        public void CanDeleteSingleCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Aviv"
                    }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                store.Operations.Send(new PatchOperation("users/1-A", null, new PatchRequest
                {
                    Script = "deleteCounter(this, args.name)",
                    Values =
                    {
                        { "name", "Downloads" }
                    }
                }));

                var countersDetail = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "Downloads" }));
                Assert.Equal(1, countersDetail.Counters.Count);
                Assert.Null(countersDetail.Counters[0]);

                using (var session = store.OpenSession())
                {
                    var u = session.Load<User>("users/1-A");
                    var counters = session.Advanced.GetCountersFor(u);

                    Assert.Null(counters);
                }
            }
        }

        [Fact]
        public void CanDeleteSingleCounterWithId()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Aviv"
                    }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.SaveChanges();
                }

                store.Operations.Send(new PatchOperation("users/1-A", null, new PatchRequest
                {
                    Script = "deleteCounter(id(this), args.name)",
                    Values =
                    {
                        { "name", "Downloads" }
                    }
                }));

                var countersDetail = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "Downloads" }));
                Assert.Equal(1, countersDetail.Counters.Count);
                Assert.Null(countersDetail.Counters[0]);
            }
        }

        [Fact]
        public void CanDeleteAllCountersOfDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Aviv"
                    }, "users/1-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/1-A").Increment("Likes", 100);
                    session.CountersFor("users/1-A").Increment("Dislikes", 100);
                    session.CountersFor("users/1-A").Increment("Score", 100);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var u = session.Load<User>("users/1-A");
                    var counters = session.Advanced.GetCountersFor(u);

                    Assert.Equal(4, counters.Count);
                }

                store.Operations.Send(new PatchOperation("users/1-A", null, new PatchRequest
                {
                    Script = @"{
                                    var counters = this[""@metadata""][""@counters""];                                    
                                    if (counters == null)
                                        return;
                                    for (var i = 0; i < counters.length; i++)
                                    {
                                        deleteCounter(id(this), counters[i]);
                                    }

                               }"
                }));

                using (var session = store.OpenSession())
                {
                    var u = session.Load<User>("users/1-A");
                    var counters = session.Advanced.GetCountersFor(u);

                    Assert.Null(counters);
                }

            }
        }

        [Fact]
        public void CanDeleteCountersViaPatchByQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "George"
                    }, "users/1-A");
                    session.Store(new User
                    {
                        Name = "John"
                    }, "users/2-A");
                    session.Store(new User
                    {
                        Name = "Paul"
                    }, "users/3-A");
                    session.Store(new User
                    {
                        Name = "Ringo"
                    }, "users/4-A");

                    session.CountersFor("users/1-A").Increment("Downloads", 100);
                    session.CountersFor("users/2-A").Increment("Downloads", 200);
                    session.CountersFor("users/3-A").Increment("Downloads", 400);
                    session.CountersFor("users/4-A").Increment("Downloads", 800);

                    session.SaveChanges();
                }

                store.Operations
                     .Send(new PatchByQueryOperation(new IndexQuery
                     {
                         Query = @"from Users as u
                                  update
                                  {
                                      var name = this[""@metadata""][""@counters""][0];
                                      deleteCounter(u, name);
                                  }"
                     })).WaitForCompletion(TimeSpan.FromMinutes(5));

                foreach (var id in new [] { "users/1-A" , "users/2-A", "users/3-A", "users/4-A" })
                {
                    var countersDetail = store.Operations
                        .Send(new GetCountersOperation(id, new[] { "Downloads" }));
                    Assert.Equal(1, countersDetail.Counters.Count);
                    Assert.Null(countersDetail.Counters[0]);
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
            public string Friend { get; set; }
        }

    }
}
