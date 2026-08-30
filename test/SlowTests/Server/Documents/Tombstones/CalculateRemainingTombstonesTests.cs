using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Tombstones
{
    public class CalculateRemainingTombstonesTests : ReplicationTestBase
    {
        public CalculateRemainingTombstonesTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Core)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task DocumentTombstones_GlobalCount(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "A" }, "users/1");
                    session.Store(new User { Name = "B" }, "users/2");
                    session.Store(new User { Name = "C" }, "users/3");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.Delete("users/2");
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sw = Stopwatch.StartNew();
                    var result = database.DocumentsStorage.GetNumberOfTombstonesToProcess(context, 0, sw, exact: true);
                    Assert.Equal(2, result.Count);
                    Assert.False(result.Estimated);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task DocumentTombstones_PerCollectionCount(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "A" }, "users/1");
                    session.Store(new User { Name = "B" }, "users/2");
                    session.Store(new Order(), "orders/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.Delete("users/2");
                    session.Delete("orders/1");
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sw = Stopwatch.StartNew();

                    var usersResult = database.DocumentsStorage.GetNumberOfTombstonesToProcess(context, "Users", 0, sw, exact: true);
                    Assert.Equal(2, usersResult.Count);

                    var ordersResult = database.DocumentsStorage.GetNumberOfTombstonesToProcess(context, "Orders", 0, sw, exact: true);
                    Assert.Equal(1, ordersResult.Count);

                    var nonExistentResult = database.DocumentsStorage.GetNumberOfTombstonesToProcess(context, "NonExistent", 0, sw, exact: true);
                    Assert.Equal(0, nonExistentResult.Count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task DocumentTombstones_PerCollectionCount_WithAfterEtag(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "A" }, "users/1");
                    session.Store(new User { Name = "B" }, "users/2");
                    session.Store(new User { Name = "C" }, "users/3");
                    session.SaveChanges();
                }

                long firstDeleteEtag;
                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = database.DocumentsStorage.GetTombstonesFrom(context, 0, 0, int.MaxValue);
                    firstDeleteEtag = 0;
                    foreach (var t in tombstones)
                    {
                        firstDeleteEtag = t.Etag;
                        break;
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("users/2");
                    session.Delete("users/3");
                    session.SaveChanges();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sw = Stopwatch.StartNew();

                    // afterEtag is exclusive - entries with etag > afterEtag are counted
                    var result = database.DocumentsStorage.GetNumberOfTombstonesToProcess(context, "Users", firstDeleteEtag, sw, exact: true);
                    Assert.Equal(2, result.Count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Counters)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CounterTombstones_GlobalCount(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "A" }, "users/1");
                    session.CountersFor("users/1").Increment("Likes", 10);
                    session.CountersFor("users/1").Increment("Dislikes", 5);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Delete("Likes");
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sw = Stopwatch.StartNew();
                    var result = database.DocumentsStorage.CountersStorage.GetNumberOfTombstonesToProcess(context, 0, sw, exact: true);
                    Assert.Equal(1, result.Count);
                    Assert.False(result.Estimated);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Counters)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CounterTombstones_PerCollectionCount(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "A" }, "users/1");
                    session.CountersFor("users/1").Increment("Likes", 10);
                    session.CountersFor("users/1").Increment("Dislikes", 5);

                    session.Store(new Order(), "orders/1");
                    session.CountersFor("orders/1").Increment("Views", 100);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Delete("Likes");
                    session.CountersFor("users/1").Delete("Dislikes");
                    session.CountersFor("orders/1").Delete("Views");
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sw = Stopwatch.StartNew();

                    var usersResult = database.DocumentsStorage.CountersStorage.GetNumberOfTombstonesToProcess(context, "Users", 0, sw, exact: true);
                    Assert.Equal(2, usersResult.Count);

                    var ordersResult = database.DocumentsStorage.CountersStorage.GetNumberOfTombstonesToProcess(context, "Orders", 0, sw, exact: true);
                    Assert.Equal(1, ordersResult.Count);

                    var nonExistentResult = database.DocumentsStorage.CountersStorage.GetNumberOfTombstonesToProcess(context, "NonExistent", 0, sw, exact: true);
                    Assert.Equal(0, nonExistentResult.Count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Counters)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CounterTombstones_PerCollectionCount_EstimatedMode(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "A" }, "users/1");
                    session.CountersFor("users/1").Increment("Likes", 10);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Delete("Likes");
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sw = Stopwatch.StartNew();
                    var result = database.DocumentsStorage.CountersStorage.GetNumberOfTombstonesToProcess(context, "Users", 0, sw, exact: false);
                    // In estimated mode the count should be >= 1 (it may be exact for small datasets)
                    Assert.True(result.Count >= 1);
                }
            }
        }

        [RavenTheory(RavenTestCategory.TimeSeries)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task TimeSeriesDeletedRangeTombstones_GlobalCount(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var baseline = DateTime.UtcNow;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "A" }, "users/1");
                    session.TimeSeriesFor("users/1", "HeartRate").Append(baseline, 70);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/1", "HeartRate").Delete(baseline.AddMinutes(-1), baseline.AddMinutes(1));
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sw = Stopwatch.StartNew();
                    var result = database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTombstonesToProcess(context, 0, sw, exact: true);
                    Assert.True(result.Count >= 1);
                    Assert.False(result.Estimated);
                }
            }
        }

        [RavenTheory(RavenTestCategory.TimeSeries)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task TimeSeriesDeletedRangeTombstones_PerCollectionCount(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var baseline = DateTime.UtcNow;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "A" }, "users/1");
                    session.TimeSeriesFor("users/1", "HeartRate").Append(baseline, 70);

                    session.Store(new Order(), "orders/1");
                    session.TimeSeriesFor("orders/1", "Temperature").Append(baseline, 22);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/1", "HeartRate").Delete(baseline.AddMinutes(-1), baseline.AddMinutes(1));
                    session.TimeSeriesFor("orders/1", "Temperature").Delete(baseline.AddMinutes(-1), baseline.AddMinutes(1));
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sw = Stopwatch.StartNew();

                    var usersResult = database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTombstonesToProcess(context, "Users", 0, sw, exact: true);
                    Assert.True(usersResult.Count >= 1);

                    var ordersResult = database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTombstonesToProcess(context, "Orders", 0, sw, exact: true);
                    Assert.True(ordersResult.Count >= 1);

                    var nonExistentResult = database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTombstonesToProcess(context, "NonExistent", 0, sw, exact: true);
                    Assert.Equal(0, nonExistentResult.Count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task DocumentTombstones_EmptyDatabase_ReturnsZero(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "any/1");

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sw = Stopwatch.StartNew();

                    var globalResult = database.DocumentsStorage.GetNumberOfTombstonesToProcess(context, 0, sw, exact: true);
                    Assert.Equal(0, globalResult.Count);

                    var collectionResult = database.DocumentsStorage.GetNumberOfTombstonesToProcess(context, "Users", 0, sw, exact: true);
                    Assert.Equal(0, collectionResult.Count);

                    var counterGlobal = database.DocumentsStorage.CountersStorage.GetNumberOfTombstonesToProcess(context, 0, sw, exact: true);
                    Assert.Equal(0, counterGlobal.Count);

                    var counterCollection = database.DocumentsStorage.CountersStorage.GetNumberOfTombstonesToProcess(context, "Users", 0, sw, exact: true);
                    Assert.Equal(0, counterCollection.Count);

                    var tsGlobal = database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTombstonesToProcess(context, 0, sw, exact: true);
                    Assert.Equal(0, tsGlobal.Count);

                    var tsCollection = database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTombstonesToProcess(context, "Users", 0, sw, exact: true);
                    Assert.Equal(0, tsCollection.Count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Counters)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task CounterTombstones_PerCollectionCount_WithAfterEtag(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "A" }, "users/1");
                    session.CountersFor("users/1").Increment("Likes", 10);
                    session.CountersFor("users/1").Increment("Dislikes", 5);
                    session.CountersFor("users/1").Increment("Views", 20);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Delete("Likes");
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                long firstTombstoneEtag;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sw = Stopwatch.StartNew();
                    var result = database.DocumentsStorage.CountersStorage.GetNumberOfTombstonesToProcess(context, "Users", 0, sw, exact: true);
                    Assert.Equal(1, result.Count);

                    // Get the etag of the first counter tombstone
                    firstTombstoneEtag = 0;
                    foreach (var item in database.DocumentsStorage.CountersStorage.GetCounterTombstonesFrom(context, 0))
                    {
                        using (item)
                        {
                            firstTombstoneEtag = item.Etag;
                            break;
                        }
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.CountersFor("users/1").Delete("Dislikes");
                    session.CountersFor("users/1").Delete("Views");
                    session.SaveChanges();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sw = Stopwatch.StartNew();

                    // All counter tombstones
                    var allResult = database.DocumentsStorage.CountersStorage.GetNumberOfTombstonesToProcess(context, "Users", 0, sw, exact: true);
                    Assert.Equal(3, allResult.Count);

                    // Only tombstones after the first one
                    var afterResult = database.DocumentsStorage.CountersStorage.GetNumberOfTombstonesToProcess(context, "Users", firstTombstoneEtag + 1, sw, exact: true);
                    Assert.Equal(2, afterResult.Count);
                }
            }
        }
    }
}
