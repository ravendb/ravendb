using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class CountersSingleNode : RavenTestBase
    {
        [Fact]
        public void IncrementCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.SaveChanges();
                }

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Delta = 0
                                }
                            }
                        }
                    }
                }));

                var val = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] {"likes"}))
                    .Counters[0]?.TotalValue;

                Assert.Equal(0, val);

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Delta = 10
                                }
                            }
                        }
                    }
                }));

                val = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(10, val);

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Delta = -3
                                }
                            }
                        }
                    }
                }));

                val = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(7, val);
            }
        }

        [Fact]
        public void GetCounterValue()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.SaveChanges();
                }

                var a = store.Operations.SendAsync(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Delta = 5
                                }
                            }
                        }
                    }
                }));

                var b = store.Operations.SendAsync(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Delta = 10
                                }
                            }
                        }
                    }
                }));

                Task.WaitAll(a, b); // run them in parallel and see that they are good

                var val = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes" }))
                    .Counters[0]?.TotalValue;

                Assert.Equal(15, val);
            }
        }

        [Fact]
        public void DeleteCounter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv1" }, "users/1-A");
                    session.Store(new User { Name = "Aviv2" }, "users/2-A");

                    session.SaveChanges();
                }

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Delta = 10
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
                                    CounterName = "likes",
                                    Delta = 20
                                }
                            }
                        }
                    }
                }));

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Delete,
                                    CounterName = "likes"
                                }
                            }
                        }
                    }
                }));

                Assert.Equal(0, store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes" }))
                    .Counters.Count);

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/2-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Delete,
                                    CounterName = "likes"
                                }
                            }
                        }
                    }
                }));

                Assert.Equal(0, store.Operations
                    .Send(new GetCountersOperation("users/2-A", new[] { "likes" }))
                    .Counters.Count);
            }
        }

        [Fact]
        public void MultiGet()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.SaveChanges();
                }

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Delta = 5
                                },
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "dislikes",
                                    Delta = 10
                                }
                            }
                        }
                    }
                }));

                var dic = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] {"likes", "dislikes"}))
                    .Counters
                    .ToDictionary(c => c.CounterName, c => c.TotalValue);

                Assert.Equal(2, dic.Count);
                Assert.Equal(5, dic["likes"]);
                Assert.Equal(10, dic["dislikes"]);
            }
        }

        [Fact]
        public void MultiSetAndGetViaBatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.Store(new User { Name = "Aviv2" }, "users/2-A");
                    session.SaveChanges();
                }

                var setBatch = new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Delta = 5
                                },
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "dislikes",
                                    Delta = 10
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
                                    CounterName = "rank",
                                    Delta = 20
                                }
                            }
                        }

                    }
                };

                store.Operations.Send(new CounterBatchOperation(setBatch));

                var getBatch = new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Get,
                                    CounterName = "likes"
                                },

                                new CounterOperation
                                {
                                    Type = CounterOperationType.Get,
                                    CounterName = "dislikes"
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
                                    Type = CounterOperationType.Get,
                                    CounterName = "rank"
                                }
                            }
                        }
                    }
                };

                var countersDetail = store.Operations.Send(new CounterBatchOperation(getBatch));

                Assert.Equal(3, countersDetail.Counters.Count);

                Assert.Equal("likes", countersDetail.Counters[0].CounterName);
                Assert.Equal("users/1-A", countersDetail.Counters[0].DocumentId);
                Assert.Equal(5, countersDetail.Counters[0].TotalValue);

                Assert.Equal("dislikes", countersDetail.Counters[1].CounterName);
                Assert.Equal("users/1-A", countersDetail.Counters[1].DocumentId);
                Assert.Equal(10, countersDetail.Counters[1].TotalValue);

                Assert.Equal("rank", countersDetail.Counters[2].CounterName);
                Assert.Equal("users/2-A", countersDetail.Counters[2].DocumentId);
                Assert.Equal(20, countersDetail.Counters[2].TotalValue);

            }
        }

        [Fact]
        public void BatchWithDifferentTypesOfOperations()
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

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Delta = 10
                                },
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "dislikes",
                                    Delta = 20
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
                                    CounterName = "rank",
                                    Delta = 30
                                }
                            }
                        },
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/3-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "score",
                                    Delta = 40
                                }
                            }
                        }

                    }
                }));

                var dic = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] {"likes", "dislikes"}))
                    .Counters
                    .ToDictionary(c => c.CounterName, c => c.TotalValue);

                Assert.Equal(2, dic.Count);
                Assert.Equal(10, dic["likes"]);
                Assert.Equal(20, dic["dislikes"]);

                var value = store.Operations
                    .Send(new GetCountersOperation("users/2-A", new [] { "rank" }))
                    .Counters[0]?.TotalValue;

                Assert.Equal(30, value);

                value = store.Operations
                    .Send(new GetCountersOperation("users/3-A", new[] { "score" }))
                    .Counters[0]?.TotalValue;

                Assert.Equal(40, value);

                var batch = new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Type = CounterOperationType.Delete,
                                    CounterName = "dislikes"
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
                                    CounterName = "rank",
                                    Delta = 200
                                },
                                new CounterOperation
                                {
                                    //create new counter
                                    Type = CounterOperationType.Increment,
                                    CounterName = "downloads",
                                    Delta = 300
                                }
                            }
                        },
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/3-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Delete,
                                    CounterName = "score"
                                }
                            }
                        }
                    }
                };

                var countersDetail = store.Operations.Send(new CounterBatchOperation(batch));

                Assert.Equal(3, countersDetail.Counters.Count);

                Assert.Equal("users/1-A", countersDetail.Counters[0].DocumentId);
                Assert.Equal("likes", countersDetail.Counters[0].CounterName);
                Assert.Equal(110, countersDetail.Counters[0].TotalValue);

                Assert.Equal("users/2-A", countersDetail.Counters[1].DocumentId);
                Assert.Equal("rank", countersDetail.Counters[1].CounterName);
                Assert.Equal(230, countersDetail.Counters[1].TotalValue);

                Assert.Equal("users/2-A", countersDetail.Counters[2].DocumentId);
                Assert.Equal("downloads", countersDetail.Counters[2].CounterName);
                Assert.Equal(300, countersDetail.Counters[2].TotalValue);

                Assert.Equal(0, store.Operations.Send(new GetCountersOperation("users/1-A", new []{ "dislikes" })).Counters.Count);
                Assert.Equal(0, store.Operations.Send(new GetCountersOperation("users/3-A", new[] { "score" })).Counters.Count);

            }
        }

        [Fact]
        public void DeleteCreateWithSameNameDeleteAgain()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");

                    session.SaveChanges();
                }

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Delta = 10
                                }
                            }
                        }
                    }
                }));

                var val = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] {"likes"}))
                    .Counters[0]?.TotalValue;

                Assert.Equal(10, val);

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Delete,
                                    CounterName = "likes"
                                }
                            }
                        }
                    }
                }));

                Assert.Equal(0, store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes" }))
                    .Counters.Count);

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Delta = 20
                                }
                            }
                        }
                    }
                }));

                val = store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes" }))
                    .Counters[0]?.TotalValue;

                Assert.Equal(20, val);

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Delete,
                                    CounterName = "likes"
                                }
                            }
                        }
                    }
                }));

                Assert.Equal(0, store.Operations
                    .Send(new GetCountersOperation("users/1-A", new[] { "likes" }))
                    .Counters.Count);
            }
        }

        [Fact]
        public void IncrementAndDeleteShouldChangeDocumentMetadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.SaveChanges();
                }

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
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
                                    Delta = 10
                                }
                            }
                        }
                    }
                }));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadatad = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadatad.TryGetValue(Constants.Documents.Metadata.Counters, out object counters));
                    Assert.Equal(1, ((object[])counters).Length);
                    Assert.True(((object[])counters).Contains("likes"));
                }

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Increment,
                                    CounterName = "votes",
                                    Delta = 50
                                }
                            }
                        }
                    }
                }));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadatad = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadatad.TryGetValue(Constants.Documents.Metadata.Counters, out object counters));
                    Assert.Equal(2, ((object[])counters).Length);
                    Assert.True(((object[])counters).Contains("likes"));
                    Assert.True(((object[])counters).Contains("votes"));
                }

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Delete,
                                    CounterName = "likes"
                                }
                            }
                        }
                    }
                }));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadatad = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadatad.TryGetValue(Constants.Documents.Metadata.Counters, out object counters));
                    Assert.Equal(1, ((object[])counters).Length);
                    Assert.True(((object[])counters).Contains("votes"));
                }

                store.Operations.Send(new CounterBatchOperation(new CounterBatch
                {
                    Documents = new List<DocumentCountersOperation>
                    {
                        new DocumentCountersOperation
                        {
                            DocumentId = "users/1-A",
                            Operations = new List<CounterOperation>
                            {
                                new CounterOperation
                                {
                                    Type = CounterOperationType.Delete,
                                    CounterName = "votes"
                                }
                            }
                        }
                    }
                }));

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var metadatad = session.Advanced.GetMetadataFor(user);
                    Assert.False(metadatad.TryGetValue(Constants.Documents.Metadata.Counters, out _));
                }

            }
        }

        [Fact]
        public void CounterNameShouldPreserveCase()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Aviv" }, "users/1-A");
                    session.CountersFor("users/1-A").Increment("Likes", 10);
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1-A");
                    var val = session.CountersFor(user).Get("Likes");
                    Assert.Equal(10, val);

                    var counters = session.Advanced.GetCountersFor(user);
                    Assert.Equal(1, counters.Count);
                    Assert.Equal("Likes", counters[0]);

                }

            }
        }

        private class UsersByAge : AbstractIndexCreationTask<User, UsersByAgeResult>
        {
            public UsersByAge()
            {
                Map = users => from user in users
                    select new
                    {
                        Age = user.Age,
                        Count = 1
                    };

                Reduce = results => from result in results
                    group result by result.Age  into g
                    select new
                    {
                        Age = g.Key,
                        Count = g.Sum(x => x.Count)
                    };

                OutputReduceToCollection = "UsersByAgeResult";
            }
        }

        private class UsersByAgeResult
        {
            public int Age { get; set; }
            public int Count { get; set; }
        }

        [Fact]
        public void ShouldThrowOnAttemptToAddCounterToArtificailDoc()
        {
            using (var store = GetDocumentStore())
            {
                new UsersByAge().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    session.Store(new User
                    {
                        Age = 21
                    }, "users/1-A");
                    session.Store(new User
                    {
                        Age = 32
                    }, "users/2-A");
                    session.Store(new User
                    {
                        Age = 32
                    }, "users/3-A");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var artificialDocs = session.Advanced.LoadStartingWith<UsersByAgeResult>("UsersByAgeResult");
                    Assert.Equal(2, artificialDocs.Length);
                    session.CountersFor(artificialDocs[0]).Increment("Likes");

                    var ex = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.Contains("Cannot put Counters on artificial documents", ex.Message);
                }
            }
        }

    }
}
