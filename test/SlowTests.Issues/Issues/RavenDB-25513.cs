using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25513(ITestOutputHelper output) : ReplicationTestBase(output)
{
    [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Replication)]
    public async Task MergeTimeSeriesOnConflict()
    {
        using (var storeA = GetDocumentStore())
        using (var storeB = GetDocumentStore())
        {
            using (var session = storeA.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                session.TimeSeriesFor("users/1", "heartbeat").Append(DateTime.Now, new List<double> { 1, 2, 3 }, "herz");
                await session.SaveChangesAsync();
            }

            await SetupReplicationAsync(storeA, storeB);
            await EnsureReplicatingAsync(storeA, storeB);

            using var r = await GetReplicationManagerAsync(storeA, storeA.Database, RavenDatabaseMode.Single);
            r.Break();

            using (var session = storeB.OpenAsyncSession())
            {
                session.Delete("users/1");
                await session.SaveChangesAsync();
            }

            string cv;
            using (var session = storeA.OpenAsyncSession())
            {
                session.TimeSeriesFor("users/1", "heartbeat").Append(DateTime.Now.AddDays(35), new List<double> { 1, 2, 3 }, "herz");
                await session.SaveChangesAsync();

                var doc = await session.LoadAsync<User>("users/1");
                cv = session.Advanced.GetChangeVectorFor(doc);
            }
            r.Mend();
            await EnsureReplicatingAsync(storeA, storeB);

            await SetupReplicationAsync(storeB, storeA);
            await EnsureReplicatingAsync(storeB, storeA);

            var statsA = await storeA.Maintenance.SendAsync(new GetEssentialStatisticsOperation());
            Assert.Equal(0, statsA.CountOfTimeSeriesSegments);

            var statsB = await storeB.Maintenance.SendAsync(new GetEssentialStatisticsOperation());
            Assert.Equal(0, statsB.CountOfTimeSeriesSegments);
        }
    }

    [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Replication)]
    public async Task RecreatedDocumentTimeSeries_ShouldReplicateAfterPreviousDeletion()
    {
        using (var storeA = GetDocumentStore())
        using (var storeB = GetDocumentStore())
        {
            var baseline = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            using (var session = storeA.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                session.TimeSeriesFor("users/1", "heartbeat")
                    .Append(baseline, new List<double> { 1, 2, 3 }, "herz");
                await session.SaveChangesAsync();
            }

            await SetupReplicationAsync(storeA, storeB);
            await EnsureReplicatingAsync(storeA, storeB);

            var preStatsB = await storeB.Maintenance.SendAsync(new GetEssentialStatisticsOperation());
            Assert.True(preStatsB.CountOfTimeSeriesSegments > 0);

            using var r = await GetReplicationManagerAsync(storeA, storeA.Database, RavenDatabaseMode.Single);
            r.Break();

            using (var session = storeB.OpenAsyncSession())
            {
                session.Delete("users/1");
                await session.SaveChangesAsync();
            }

            r.Mend();
            await EnsureReplicatingAsync(storeA, storeB);

            await SetupReplicationAsync(storeB, storeA);
            await EnsureReplicatingAsync(storeB, storeA);

            using (var session = storeA.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<User>("users/1");
                Assert.Null(doc);
            }

            // A: recreate doc + append new TS
            using (var session = storeA.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Recreated" }, "users/1");
                session.TimeSeriesFor("users/1", "heartbeat")
                    .Append(baseline.AddDays(1), new List<double> { 10, 20, 30 }, "herz");
                await session.SaveChangesAsync();
            }

            await EnsureReplicatingAsync(storeA, storeB);

            // verify that B has the recreated doc + the new TS entry
            using (var session = storeB.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<User>("users/1");
                Assert.NotNull(doc);
                Assert.Equal("Recreated", doc.Name);

                var entries = await session.TimeSeriesFor("users/1", "heartbeat")
                    .GetAsync(baseline.AddDays(1), baseline.AddDays(1));

                Assert.NotNull(entries);
                Assert.Equal(1, entries.Length);
                Assert.Equal(baseline.AddDays(1), entries[0].Timestamp);
            }

            var statsB = await storeB.Maintenance.SendAsync(new GetEssentialStatisticsOperation());
            Assert.True(statsB.CountOfTimeSeriesSegments > 0);
        }
    }

    [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Replication)]
    public async Task DocDeletionShouldPreventReintroducingPreviouslyExistingSegments()
    {
        using (var storeA = GetDocumentStore())
        using (var storeB = GetDocumentStore())
        {
            var baseline = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc);

            using (var session = storeA.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Karmel" }, "users/1");
                session.TimeSeriesFor("users/1", "heartbeat")
                    .Append(baseline, new List<double> { 1, 2, 3 }, "herz");
                await session.SaveChangesAsync();
            }

            await SetupReplicationAsync(storeA, storeB);
            await EnsureReplicatingAsync(storeA, storeB);

            var preStatsB = await storeB.Maintenance.SendAsync(new GetEssentialStatisticsOperation());
            Assert.True(preStatsB.CountOfTimeSeriesSegments > 0);

            using var r = await GetReplicationManagerAsync(storeA, storeA.Database, RavenDatabaseMode.Single);
            r.Break();

            // B: delete doc
            using (var session = storeB.OpenAsyncSession())
            {
                session.Delete("users/1");
                await session.SaveChangesAsync();
            }

            // A: modify the segment while replication is broken
            using (var session = storeA.OpenAsyncSession())
            {
                session.TimeSeriesFor("users/1", "heartbeat")
                    .Append(baseline.AddMinutes(10), new List<double> { 4, 5, 6 }, "herz");
                await session.SaveChangesAsync();
            }

            r.Mend();
            await EnsureReplicatingAsync(storeA, storeB);

            // replicate deletion back to A
            await SetupReplicationAsync(storeB, storeA);
            await EnsureReplicatingAsync(storeB, storeA);

            var statsA = await storeA.Maintenance.SendAsync(new GetEssentialStatisticsOperation());
            Assert.Equal(0, statsA.CountOfTimeSeriesSegments);

            var statsB = await storeB.Maintenance.SendAsync(new GetEssentialStatisticsOperation());
            Assert.Equal(0, statsB.CountOfTimeSeriesSegments);
        }
    }


}
