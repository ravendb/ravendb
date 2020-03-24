using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations.Counters;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Counters
{
    public class BulkInsertCounters : RavenTestBase
    {
        public BulkInsertCounters(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void IncrementCounter()
        {
            using (var store = GetDocumentStore())
            {
                string userId1;
                string userId2;
                using (var bulkInsert = store.BulkInsert())
                {
                    var user1 = new User {Name = "Aviv1"};
                    bulkInsert.Store(user1);
                    userId1 = user1.Id;

                    var user2 = new User {Name = "Aviv2"};
                    bulkInsert.Store(user2);
                    userId2 = user2.Id;

                    using (var countersBulkInsert = bulkInsert.CountersFor(userId1))
                    {
                        countersBulkInsert.Increment("likes", 100);
                        countersBulkInsert.Increment("downloads", 500);
                    }

                    using (var countersBulkInsert = bulkInsert.CountersFor(userId2))
                    {
                        countersBulkInsert.Increment("votes", 1000);
                    }
                }

                var dic = store.Operations
                    .Send(new GetCountersOperation(userId1, new[] { "likes", "downloads" }))
                    .Counters
                    .ToDictionary(c => c.CounterName, c => c.TotalValue);

                Assert.Equal(2, dic.Count);

                Assert.Equal(100, dic["likes"]);
                Assert.Equal(500, dic["downloads"]);

                var val = store.Operations
                    .Send(new GetCountersOperation(userId2, new[] { "votes" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(1000, val);
            }
        }

        [Fact]
        public void IncrementCounterInSeparateBulkInserts()
        {
            using (var store = GetDocumentStore())
            {
                string userId1;
                string userId2;
                using (var bulkInsert = store.BulkInsert())
                {
                    var user1 = new User { Name = "Aviv1" };
                    bulkInsert.Store(user1);
                    userId1 = user1.Id;

                    var user2 = new User { Name = "Aviv2" };
                    bulkInsert.Store(user2);
                    userId2 = user2.Id;
                }
                
                using (var bulkInsert = store.BulkInsert())
                {
                    using (var countersBulkInsert = bulkInsert.CountersFor(userId1))
                    {
                        countersBulkInsert.Increment("likes", 100);
                        countersBulkInsert.Increment("downloads", 500);
                    }

                    using (var countersBulkInsert = bulkInsert.CountersFor(userId2))
                    {
                        countersBulkInsert.Increment("votes", 1000);
                    }
                }

                var dic = store.Operations
                    .Send(new GetCountersOperation(userId1, new[] { "likes", "downloads" }))
                    .Counters
                    .ToDictionary(c => c.CounterName, c => c.TotalValue);

                Assert.Equal(2, dic.Count);

                Assert.Equal(100, dic["likes"]);
                Assert.Equal(500, dic["downloads"]);

                var val = store.Operations
                    .Send(new GetCountersOperation(userId2, new[] { "votes" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(1000, val);
            }
        }

        [Fact]
        public void IncrementCounterErrors()
        {
            using (var store = GetDocumentStore())
            {
                string userId1;
                string userId2;
                using (var bulkInsert = store.BulkInsert())
                {
                    var user1 = new User { Name = "Aviv1" };
                    bulkInsert.Store(user1);
                    userId1 = user1.Id;

                    var user2 = new User { Name = "Aviv2" };
                    bulkInsert.Store(user2);
                    userId2 = user2.Id;

                    using (var countersBulkInsert = bulkInsert.CountersFor(userId1))
                    {
                        var exception = Assert.Throws<InvalidOperationException>(() => bulkInsert.CountersFor(userId2));
                        Assert.Equal("An ongoing bulk insert operation of type 'Counters' is already running Did you forget to Dispose() the command?", exception.Message);

                        exception = Assert.Throws<InvalidOperationException>(() => bulkInsert.Store(new User()));
                        Assert.Equal("An ongoing 'Counters' bulk insert operation is already running while the new operation is 'PUT', did you forget to Dispose() the previous operation?", exception.Message);

                        countersBulkInsert.Increment("likes", 100);
                        countersBulkInsert.Increment("downloads", 500);
                    }

                    using (var countersBulkInsert = bulkInsert.CountersFor(userId2))
                    {
                        var exception = Assert.Throws<InvalidOperationException>(() => bulkInsert.CountersFor(userId1));
                        Assert.Equal("An ongoing bulk insert operation of type 'Counters' is already running Did you forget to Dispose() the command?", exception.Message);

                        exception = Assert.Throws<InvalidOperationException>(() => bulkInsert.Store(new User()));
                        Assert.Equal("An ongoing 'Counters' bulk insert operation is already running while the new operation is 'PUT', did you forget to Dispose() the previous operation?", exception.Message);

                        countersBulkInsert.Increment("votes", 1000);
                    }
                }

                var dic = store.Operations
                    .Send(new GetCountersOperation(userId1, new[] { "likes", "downloads" }))
                    .Counters
                    .ToDictionary(c => c.CounterName, c => c.TotalValue);

                Assert.Equal(2, dic.Count);

                Assert.Equal(100, dic["likes"]);
                Assert.Equal(500, dic["downloads"]);

                var val = store.Operations
                    .Send(new GetCountersOperation(userId2, new[] { "votes" }))
                    .Counters[0]?.TotalValue;
                Assert.Equal(1000, val);

                var disposeError = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        var countersBulkInsert = bulkInsert.CountersFor("test");
                        countersBulkInsert.Increment("votes", 1000);
                    }
                });
                Assert.Equal("An ongoing bulk insert operation of type 'Counters' is already running Did you forget to Dispose() the command?", disposeError.Message);
                
                disposeError = Assert.Throws<InvalidOperationException>(() =>
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        var countersBulkInsert = bulkInsert.CountersFor("test");
                        countersBulkInsert.Dispose();
                        countersBulkInsert.Increment("votes", 1000);
                    }
                });
                Assert.Equal($"Cannot increment counter 'votes' because {nameof(BulkInsertOperation.CountersBulkInsert)} was already disposed", disposeError.Message);

                var argumentError = Assert.Throws<ArgumentException>(() =>
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        bulkInsert.CountersFor(null);
                    }
                });
                Assert.Equal("Document id cannot be null or empty (Parameter 'id')", argumentError.Message);
            }
        }

        [Theory]
        [InlineData(128)]
        [InlineData(1000)]
        [InlineData(10_000)]
        [InlineData(50_000)]
        [InlineData(100_000)]
        public void IncrementManyCounters(int counterCount)
        {
            using (var store = GetDocumentStore())
            {
                string userId1;
                using (var bulkInsert = store.BulkInsert())
                {
                    var user1 = new User { Name = "Aviv1" };
                    bulkInsert.Store(user1);
                    userId1 = user1.Id;

                    using (var countersBulkInsert = bulkInsert.CountersFor(userId1))
                    {
                        for (var i = 1; i < counterCount + 1; i++)
                        {
                            countersBulkInsert.Increment(i.ToString(), i);
                        }
                    }
                }

                var dictionary = store.Operations
                    .Send(new GetCountersOperation(userId1))
                    .Counters
                    .ToDictionary(c => c.CounterName, c => c.TotalValue);

                Assert.Equal(counterCount, dictionary.Count);

                for (var i = 1; i < counterCount + 1; i++)
                {
                    Assert.Equal(i, dictionary[i.ToString()]);
                }
            }
        }
    }
}
