using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter;
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

                    var usersResult = database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRangesToProcess(context, "Users", 0, sw, exact: true);
                    Assert.True(usersResult.Count >= 1);

                    var ordersResult = database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRangesToProcess(context, "Orders", 0, sw, exact: true);
                    Assert.True(ordersResult.Count >= 1);

                    var nonExistentResult = database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRangesToProcess(context, "NonExistent", 0, sw, exact: true);
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

                    var tsCollection = database.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesDeletedRangesToProcess(context, "Users", 0, sw, exact: true);
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

                    // afterEtag is exclusive - only the tombstones created after the first one are counted
                    var afterResult = database.DocumentsStorage.CountersStorage.GetNumberOfTombstonesToProcess(context, "Users", firstTombstoneEtag, sw, exact: true);
                    Assert.Equal(2, afterResult.Count);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Core)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task TombstonesState_DocumentTombstones_CountsTombstonesAfterLastProcessedEtag(Options options)
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
                    session.Delete("users/3");
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                List<long> tombstoneEtags;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    tombstoneEtags = database.DocumentsStorage.GetTombstonesFrom(context, 0, 0, long.MaxValue).Select(x => x.Etag).ToList();
                }

                Assert.Equal(3, tombstoneEtags.Count);

                // the last processed etag is right before the second tombstone, so the second and the third tombstones are still blocking the cleanup
                var lastProcessedEtag = tombstoneEtags[1] - 1;

                using (var perCollection = new TestTombstoneAwareSubscription("per-collection", ITombstoneAware.TombstoneType.Documents, "Users", lastProcessedEtag).Subscribe(database))
                using (var allCollections = new TestTombstoneAwareSubscription("all-collections", ITombstoneAware.TombstoneType.Documents, Constants.Documents.Collections.AllDocumentsCollection, lastProcessedEtag).Subscribe(database))
                using (var upToDate = new TestTombstoneAwareSubscription("up-to-date", ITombstoneAware.TombstoneType.Documents, "Users", tombstoneEtags[2]).Subscribe(database))
                using (var notStarted = new TestTombstoneAwareSubscription("not-started", ITombstoneAware.TombstoneType.Documents, "Users", 0).Subscribe(database))
                {
                    var state = database.TombstoneCleaner.GetState(addInfoForDebug: true, exact: true);

                    AssertRemainingTombstones(state, perCollection, expected: 2);
                    AssertRemainingTombstones(state, allCollections, expected: 2);
                    AssertRemainingTombstones(state, upToDate, expected: 0);
                    AssertRemainingTombstones(state, notStarted, expected: 3);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Counters)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task TombstonesState_CounterTombstones_CountsTombstonesAfterLastProcessedEtag(Options options)
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
                    session.CountersFor("users/1").Delete("Dislikes");
                    session.CountersFor("users/1").Delete("Views");
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                var tombstoneEtags = new List<long>();
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    foreach (var item in database.DocumentsStorage.CountersStorage.GetCounterTombstonesFrom(context, 0))
                    {
                        using (item)
                            tombstoneEtags.Add(item.Etag);
                    }
                }

                Assert.Equal(3, tombstoneEtags.Count);

                // the last processed etag is right before the second tombstone, so the second and the third tombstones are still blocking the cleanup
                var lastProcessedEtag = tombstoneEtags[1] - 1;

                using (var perCollection = new TestTombstoneAwareSubscription("per-collection", ITombstoneAware.TombstoneType.Counters, "Users", lastProcessedEtag).Subscribe(database))
                using (var allCollections = new TestTombstoneAwareSubscription("all-collections", ITombstoneAware.TombstoneType.Counters, Constants.Counters.All, lastProcessedEtag).Subscribe(database))
                using (var upToDate = new TestTombstoneAwareSubscription("up-to-date", ITombstoneAware.TombstoneType.Counters, "Users", tombstoneEtags[2]).Subscribe(database))
                {
                    var state = database.TombstoneCleaner.GetState(addInfoForDebug: true, exact: true);

                    AssertRemainingTombstones(state, perCollection, expected: 2);
                    AssertRemainingTombstones(state, allCollections, expected: 2);
                    AssertRemainingTombstones(state, upToDate, expected: 0);
                }
            }
        }

        [RavenTheory(RavenTestCategory.TimeSeries)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task TombstonesState_TimeSeriesDeletedRanges_CountsTombstonesAfterLastProcessedEtag(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var baseline = DateTime.UtcNow;

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "A" }, "users/1");
                    session.TimeSeriesFor("users/1", "HeartRate").Append(baseline, 70);
                    session.TimeSeriesFor("users/1", "BloodPressure").Append(baseline, 120);
                    session.TimeSeriesFor("users/1", "Temperature").Append(baseline, 37);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/1", "HeartRate").Delete(baseline.AddMinutes(-1), baseline.AddMinutes(1));
                    session.TimeSeriesFor("users/1", "BloodPressure").Delete(baseline.AddMinutes(-1), baseline.AddMinutes(1));
                    session.TimeSeriesFor("users/1", "Temperature").Delete(baseline.AddMinutes(-1), baseline.AddMinutes(1));
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                var tombstoneEtags = new List<long>();
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    foreach (var item in database.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesFrom(context, 0))
                    {
                        using (item)
                            tombstoneEtags.Add(item.Etag);
                    }
                }

                Assert.True(tombstoneEtags.Count >= 3, $"Expected at least 3 deleted ranges but got {tombstoneEtags.Count}");

                // the last processed etag is right before the second deleted range, so all the deleted ranges but the first one are still blocking the cleanup
                var lastProcessedEtag = tombstoneEtags[1] - 1;
                var expected = tombstoneEtags.Count - 1;

                using (var perCollection = new TestTombstoneAwareSubscription("per-collection", ITombstoneAware.TombstoneType.TimeSeries, "Users", lastProcessedEtag).Subscribe(database))
                using (var allCollections = new TestTombstoneAwareSubscription("all-collections", ITombstoneAware.TombstoneType.TimeSeries, Constants.TimeSeries.All, lastProcessedEtag).Subscribe(database))
                using (var upToDate = new TestTombstoneAwareSubscription("up-to-date", ITombstoneAware.TombstoneType.TimeSeries, "Users", tombstoneEtags[^1]).Subscribe(database))
                {
                    var state = database.TombstoneCleaner.GetState(addInfoForDebug: true, exact: true);

                    AssertRemainingTombstones(state, perCollection, expected);
                    AssertRemainingTombstones(state, allCollections, expected);
                    AssertRemainingTombstones(state, upToDate, expected: 0);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task AttachmentTombstones_PseudoCollectionCount(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                using (var stream1 = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var stream2 = new MemoryStream(new byte[] { 4, 5, 6 }))
                using (var stream3 = new MemoryStream(new byte[] { 7, 8, 9 }))
                {
                    session.Store(new User { Name = "A" }, "users/1");
                    session.Advanced.Attachments.Store("users/1", "file1", stream1);
                    session.Advanced.Attachments.Store("users/1", "file2", stream2);
                    session.Advanced.Attachments.Store("users/1", "file3", stream3);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Delete("users/1", "file1");
                    session.Advanced.Attachments.Delete("users/1", "file2");
                    session.Advanced.Attachments.Delete("users/1", "file3");
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceForAsync(store, options.DatabaseMode, "users/1");

                List<long> tombstoneEtags;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    tombstoneEtags = database.DocumentsStorage.GetTombstonesFrom(context, AttachmentsTombstones, 0, 0, long.MaxValue).Select(x => x.Etag).ToList();
                    Assert.Equal(3, tombstoneEtags.Count);

                    var sw = Stopwatch.StartNew();

                    var allResult = database.DocumentsStorage.GetNumberOfTombstonesToProcess(context, AttachmentsTombstones, 0, sw, exact: true);
                    Assert.Equal(3, allResult.Count);
                    Assert.False(allResult.Estimated);

                    var afterResult = database.DocumentsStorage.GetNumberOfTombstonesToProcess(context, AttachmentsTombstones, tombstoneEtags[0], sw, exact: true);
                    Assert.Equal(2, afterResult.Count);
                }

                // a Raven ETL task with an empty script registers the attachment tombstones pseudo collection (see EtlLoader.MarkDocumentTombstonesForDeletion)
                var lastProcessedEtag = tombstoneEtags[1] - 1;

                using (var etl = new TestTombstoneAwareSubscription("etl", ITombstoneAware.TombstoneType.Documents, AttachmentsTombstones, lastProcessedEtag).Subscribe(database))
                using (var upToDate = new TestTombstoneAwareSubscription("up-to-date", ITombstoneAware.TombstoneType.Documents, AttachmentsTombstones, tombstoneEtags[2]).Subscribe(database))
                {
                    var state = database.TombstoneCleaner.GetState(addInfoForDebug: true, exact: true);

                    AssertRemainingTombstones(state, etl, expected: 2);
                    AssertRemainingTombstones(state, upToDate, expected: 0);
                }
            }
        }

        private static string AttachmentsTombstones => Raven.Server.Documents.Schemas.Attachments.AttachmentsTombstones;

        private static void AssertRemainingTombstones(TombstoneCleaner.TombstonesState state, TestTombstoneAwareSubscription subscription, long expected, bool estimated = false)
        {
            Assert.True(state.PerSubscriptionInfoExtended.TryGetValue(subscription.StateKey, out var info), $"Missing state for {subscription.StateKey}");

            Assert.Equal(expected, info.NumberOfTombstoneLeft);
            Assert.Equal(estimated, info.Estimated);
            Assert.Equal(expected, subscription.TombstoneType switch
            {
                ITombstoneAware.TombstoneType.Documents => info.Types.Documents,
                ITombstoneAware.TombstoneType.Counters => info.Types.Counters,
                ITombstoneAware.TombstoneType.TimeSeries => info.Types.TimeSeries,
                _ => throw new ArgumentOutOfRangeException(nameof(subscription.TombstoneType), subscription.TombstoneType, null)
            });
        }

        private sealed class TestTombstoneAwareSubscription : ITombstoneAware, IDisposable
        {
            private readonly string _collection;
            private readonly long _lastProcessedEtag;
            private DocumentDatabase _database;

            public TestTombstoneAwareSubscription(string identifier, ITombstoneAware.TombstoneType tombstoneType, string collection, long lastProcessedEtag)
            {
                TombstoneCleanerIdentifier = identifier;
                TombstoneType = tombstoneType;
                _collection = collection;
                _lastProcessedEtag = lastProcessedEtag;
            }

            public string TombstoneCleanerIdentifier { get; }

            public ITombstoneAware.TombstoneType TombstoneType { get; }

            // the key used by TombstonesState.AddPerSubscriptionInfoExtended
            public string StateKey
            {
                get
                {
                    var collection = _collection == Constants.Documents.Collections.AllDocumentsCollection ||
                                     _collection == Constants.Counters.All ||
                                     _collection == Constants.TimeSeries.All
                        ? string.Empty
                        : _collection;

                    return $"{TombstoneCleanerIdentifier}/{TombstoneCleanerIdentifier}/{collection}";
                }
            }

            public TestTombstoneAwareSubscription Subscribe(DocumentDatabase database)
            {
                _database = database;
                _database.TombstoneCleaner.Subscribe(this);
                return this;
            }

            public Dictionary<string, long> GetLastProcessedTombstonesPerCollection(ITombstoneAware.TombstoneType tombstoneType, Dictionary<string, LastTombstoneInfo> lastProcessedTombstonesInfo = null)
            {
                if (tombstoneType != TombstoneType)
                    return null;

                lastProcessedTombstonesInfo?.Add(_collection, new LastTombstoneInfo(TombstoneCleanerIdentifier, _collection, _lastProcessedEtag, ITombstoneAware.TombstoneDeletionBlockerType.RavenEtl));

                return new Dictionary<string, long>
                {
                    [_collection] = _lastProcessedEtag
                };
            }

            public Dictionary<TombstoneDeletionBlockageSource, HashSet<string>> GetDisabledSubscribersCollections(HashSet<string> tombstoneCollections)
            {
                return new Dictionary<TombstoneDeletionBlockageSource, HashSet<string>>();
            }

            public void Dispose()
            {
                _database?.TombstoneCleaner.Unsubscribe(this);
            }
        }
    }
}
