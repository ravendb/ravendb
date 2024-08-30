using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace InterversionTests
{
    public class RavenDB_17518 : InterversionTestBase
    {
        public RavenDB_17518(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Replication | RavenTestCategory.Counters, RavenPlatform.Windows)]
        public async Task ShouldNotReplicateCountersToOldServer()
        {
            const string docId = "users/1";
            using (var oldStore = await GetDocumentStoreAsync("4.0.7"))
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, docId);
                    session.CountersFor(docId)
                        .Increment("likes");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(docId);
                    session.CountersFor(u)
                        .Increment("likes");

                    u.Name = "oren";

                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store, oldStore);

                var replicationLoader = (await Databases.GetDocumentDatabaseInstanceFor(store)).ReplicationLoader;
                Assert.NotEmpty(replicationLoader.OutgoingFailureInfo);
                Assert.True(WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException))));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains("CounterGroup"))));
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Replication | RavenTestCategory.TimeSeries, RavenPlatform.Windows)]
        public async Task ShouldNotReplicateTimeSeriesToOldServer()
        {
            const string docId = "users/1";
            using (var oldStore = await GetDocumentStoreAsync("4.2.117"))
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, docId);
                    session.TimeSeriesFor(docId, "HeartRate")
                        .Append(DateTime.UtcNow, 1);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(docId);
                    session.TimeSeriesFor(u, "HeartRate")
                        .Append(DateTime.UtcNow, 1);

                    u.Name = "oren";

                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store, oldStore);

                var replicationLoader = (await Databases.GetDocumentDatabaseInstanceFor(store)).ReplicationLoader;
                Assert.NotEmpty(replicationLoader.OutgoingFailureInfo);
                Assert.True(WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException))));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains("TimeSeries"))));
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Replication | RavenTestCategory.Counters, RavenPlatform.Windows)]
        public async Task ShouldNotReplicateCounterTombstonesToOldServer()
        {
            const string docId = "users/1";
            using (var oldStore = await GetDocumentStoreAsync("4.2.117"))
            using (var store = GetDocumentStore())
            {

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, docId);
                    var cf = session.CountersFor(docId);
                    cf.Increment("Likes", 1);
                    cf.Increment("Likes2", 1);
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store, oldStore);
                await EnsureReplicatingAsync(store, oldStore);

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(docId);
                    session.CountersFor(u)
                        .Delete("Likes");

                    await session.SaveChangesAsync();
                }

                await EnsureReplicatingAsync(store, oldStore);

                using (var session = oldStore.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(docId);
                    var counters = await session.CountersFor(u).GetAllAsync();

                    Assert.Equal(1, counters.Count);
                }

                var storage = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(1, c2);
                }

                var cleaner = storage.TombstoneCleaner;
                await cleaner.ExecuteCleanup();

                using (storage.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    var c2 = storage.DocumentsStorage.CountersStorage.GetNumberOfCounterTombstoneEntries(context);
                    Assert.Equal(0, c2);
                }
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Interversion | RavenTestCategory.Replication | RavenTestCategory.TimeSeries, RavenPlatform.Windows | RavenPlatform.Linux)]
        public async Task ShouldNotReplicateIncrementalTimeSeriesToOldServer2()
        {
            const string version = Server52Version;
            const string incrementalTsName = Constants.Headers.IncrementalTimeSeriesPrefix + "HeartRate";
            const string docId = "users/1";
            var baseline = DateTime.UtcNow;

            using (var oldStore = await GetDocumentStoreAsync(version))
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "ayende" }, docId);
                    session.IncrementalTimeSeriesFor(docId, incrementalTsName)
                        .Increment(baseline, 1);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var u = await session.LoadAsync<User>(docId);
                    session.IncrementalTimeSeriesFor(u, incrementalTsName)
                        .Increment(baseline, 1);

                    u.Name = "oren";

                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store, oldStore);

                ReplicationLoader replicationLoader = null;
                await WaitForValueAsync(async () =>
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                    replicationLoader = database.ReplicationLoader;
                    return replicationLoader.OutgoingFailureInfo?.Count > 0;
                }, true);

                Assert.NotNull(replicationLoader);
                Assert.NotEmpty(replicationLoader.OutgoingFailureInfo);
                Assert.True(WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException))));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains("IncrementalTimeSeries"))));
            }
        }
    }
}
