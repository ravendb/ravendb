using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Operations.Counters;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Counters
{
    public class BulkInsertCounters : RavenTestBase
    {
        public BulkInsertCounters(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void IncrementCounter(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                    var counter = bulkInsert.CountersFor(userId1);
                    counter.Increment("likes", 100);
                    counter.Increment("downloads", 500);
                    bulkInsert.CountersFor(userId2).Increment("votes", 1000);
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

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void AddDocumentAfterIncrementCounter(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                string userId1;
                string userId2;
                using (var bulkInsert = store.BulkInsert())
                {
                    var user1 = new User { Name = "Grisha" };
                    bulkInsert.Store(user1);
                    userId1 = user1.Id;
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    var counter = bulkInsert.CountersFor(userId1);
                    counter.Increment("likes", 100);
                    counter.Increment("downloads", 500);

                    var user2 = new User { Name = "Kotler" };
                    bulkInsert.Store(user2);
                    userId2 = user2.Id;
                    bulkInsert.CountersFor(userId2).Increment("votes", 1000);
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

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void IncrementCounterInSeparateBulkInserts(Options options)
        {
            using (var store = GetDocumentStore(options))
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
                    var counter = bulkInsert.CountersFor(userId1);
                    counter.Increment("likes", 100);
                    bulkInsert.CountersFor(userId2).Increment("votes", 1000);
                    counter.Increment("downloads", 500);
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

        [RavenFact(RavenTestCategory.BulkInsert)]
        public void IncrementCounterNullId()
        {
            using (var store = GetDocumentStore())
            {
                var argumentError = Assert.Throws<ArgumentException>(() =>
                {
                    using (var bulkInsert = store.BulkInsert())
                    {
                        bulkInsert.CountersFor(null).Increment("votes", 1000);
                    }
                });

                Assert.Equal("Document id cannot be null or empty (Parameter 'id')", argumentError.Message);
            }
        }

        [RavenTheory(RavenTestCategory.BulkInsert)]
        [RavenData(128, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(1000, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(10_000, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(50_000, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(100_000, DatabaseMode = RavenDatabaseMode.All)]
        public void IncrementManyCounters(Options options, int counterCount)
        {
            using (var store = GetDocumentStore(options))
            {
                string userId1;
                using (var bulkInsert = store.BulkInsert())
                {
                    var user1 = new User { Name = "Aviv1" };
                    bulkInsert.Store(user1);
                    userId1 = user1.Id;

                    var counter = bulkInsert.CountersFor(userId1);
                    for (var i = 1; i < counterCount + 1; i++)
                    {
                        counter.Increment( i.ToString(), i);
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
