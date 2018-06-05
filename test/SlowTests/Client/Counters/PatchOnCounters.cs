using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Xunit;
using PatchRequest = Raven.Client.Documents.Operations.PatchRequest;

namespace SlowTests.Client.Counters
{
    public class PatchOnCounters : RavenTestBase
    {
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
                    session.Advanced.Counters.Increment("users/1-A", "Downloads", 100);
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

                var val = store.Counters.Get("users/1-A", "Downloads");

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
                    session.Advanced.Counters.Increment("users/1-A", "Downloads", 100);
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

                var val = store.Counters.Get("users/1-A", "Downloads");

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
                    session.Advanced.Counters.Increment("users/1-A", "Downloads", 100);
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

                var val = store.Counters.Get("users/1-A", "Likes");
                Assert.Equal(200, val);

                using (var session = store.OpenSession())
                {
                    var u = session.Load<User>("users/1-A");
                    var counters = session.Advanced.GetCountersFor(u);
                    Assert.Equal(2, counters.Count);
                    Assert.Contains("Downloads", counters);
                    Assert.Contains("Likes", counters);
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
                    session.Advanced.Counters.Increment("users/1-A", "Downloads", 100);
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

                var val = store.Counters.Get("users/1-A", "Downloads");

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

                    session.Advanced.Counters.Increment("users/1-A", "Score", 100);
                    session.Advanced.Counters.Increment("users/2-A", "Score", 300);
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

                var val = store.Counters.Get("users/1-A", "Score");
                Assert.Equal(150, val);
                val = store.Counters.Get("users/2-A", "Score");
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

                    session.Advanced.Counters.Increment("users/1-A", "Downloads", 100);
                    session.Advanced.Counters.Increment("users/2-A", "Downloads", 200);
                    session.Advanced.Counters.Increment("users/3-A", "Downloads", 400);
                    session.Advanced.Counters.Increment("users/4-A", "Downloads", 800);

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
                     })).WaitForCompletion();



                var val = store.Counters.Get("users/1-A", "Downloads");
                Assert.Equal(200, val);

                val = store.Counters.Get("users/2-A", "Downloads");
                Assert.Equal(300, val);

                val = store.Counters.Get("users/3-A", "Downloads");
                Assert.Equal(500, val);

                val = store.Counters.Get("users/4-A", "Downloads");
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
                    session.Advanced.Counters.Increment("users/1-A", "Downloads", 100);
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

                var val = store.Counters.Get("users/1-A", "Downloads");
                Assert.Null(val);

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
                    session.Advanced.Counters.Increment("users/1-A", "Downloads", 100);
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

                var val = store.Counters.Get("users/1-A", "Downloads");

                Assert.Null(val);
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

                    session.Advanced.Counters.Increment("users/1-A", "Downloads", 100);
                    session.Advanced.Counters.Increment("users/1-A", "Likes", 100);
                    session.Advanced.Counters.Increment("users/1-A", "Dislikes", 100);
                    session.Advanced.Counters.Increment("users/1-A", "Score", 100);

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

                    session.Advanced.Counters.Increment("users/1-A", "Downloads", 100);
                    session.Advanced.Counters.Increment("users/2-A", "Downloads", 200);
                    session.Advanced.Counters.Increment("users/3-A", "Downloads", 400);
                    session.Advanced.Counters.Increment("users/4-A", "Downloads", 800);

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
                     })).WaitForCompletion();



                var val = store.Counters.Get("users/1-A", "Downloads");
                Assert.Null(val);

                val = store.Counters.Get("users/2-A", "Downloads");
                Assert.Null(val);

                val = store.Counters.Get("users/3-A", "Downloads");
                Assert.Null(val);

                val = store.Counters.Get("users/4-A", "Downloads");
                Assert.Null(val);
            }
        }

        private class User
        {
            public string Name { get; set; }
            public string Friend { get; set; }
        }

    }
}
