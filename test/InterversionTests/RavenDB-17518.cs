using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static InterversionTests.ReplicationTests;

namespace InterversionTests
{
    public class RavenDB_17518 : InterversionTestBase
    {
        public RavenDB_17518(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
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

                await SetupReplication(store, oldStore);

                var replicationLoader = (await GetDocumentDatabaseInstanceFor(store)).ReplicationLoader;
                Assert.NotEmpty(replicationLoader.OutgoingFailureInfo);
                Assert.True(WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException))));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains("CounterGroup"))));
            }
        }

        [Fact]
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

                await SetupReplication(store, oldStore);

                var replicationLoader = (await GetDocumentDatabaseInstanceFor(store)).ReplicationLoader;
                Assert.NotEmpty(replicationLoader.OutgoingFailureInfo);
                Assert.True(WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException))));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains("TimeSeries"))));
            }
        }

        [Fact]
        public async Task ShouldNotReplicateIncrementalTimeSeriesToOldServer2()
        {
            const string version = "5.2.3";
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

                await SetupReplication(store, oldStore);

                var replicationLoader = (await GetDocumentDatabaseInstanceFor(store)).ReplicationLoader;
                Assert.NotEmpty(replicationLoader.OutgoingFailureInfo);
                Assert.True(WaitForValue(() => replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.RetriesCount > 2), true));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Any(x => x.GetType() == typeof(LegacyReplicationViolationException))));
                Assert.True(replicationLoader.OutgoingFailureInfo.Any(ofi => ofi.Value.Errors.Select(x => x.Message).Any(x => x.Contains("IncrementalTimeSeries"))));
            }
        }
    }
}
