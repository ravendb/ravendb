using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_26788 : ClusterTestBase
    {
        public RavenDB_26788(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Sharding)]
        public async Task LocalDeletedRangeUpdateShouldSupersedeMigratedDeletedRange()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var baseline = RavenTestHelper.UtcToday;
                const string id = "users/ayende";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), id);

                    var tsf = session.TimeSeriesFor(id, "Heartrate");
                    tsf.Append(baseline.AddMinutes(1), 59d);
                    tsf.Append(baseline.AddMinutes(2), 69d);
                    tsf.Append(baseline.AddMinutes(3), 79d);
                    session.SaveChanges();
                }

                // a deleted range authored on the original shard
                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor(id, "Heartrate").Delete(baseline.AddMinutes(1), baseline.AddMinutes(2));
                    session.SaveChanges();
                }

                // bucket migration rewrites the deleted-range change vector into the composite 'order|version' shape
                await Sharding.Resharding.MoveShardForId(store, id);

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);
                var newShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, newLocation));

                string migratedRangeChangeVector;
                using (newShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    migratedRangeChangeVector = newShard.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesFrom(ctx, 0).Single().ChangeVector;
                }

                // precondition: the migrated deleted range carries a composite change vector
                Assert.Contains("|", migratedRangeChangeVector);

                // a local delete that overlaps the migrated range must supersede it, not conflict with it
                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor(id, "Heartrate").Delete(baseline.AddMinutes(1), baseline.AddMinutes(3));
                    session.SaveChanges();
                }

                using (newShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var latestDeletedRange = newShard.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesFrom(ctx, 0).OrderByDescending(x => x.Etag).First();
                    var status = ChangeVector.GetConflictStatus(ctx, migratedRangeChangeVector, latestDeletedRange.ChangeVector);
                    Assert.Equal(ConflictStatus.AlreadyMerged, status);
                }
            }
        }

        // Simulate a source-replica failover after bucket ownership moved to shard 1 but before shard 0
        // cleaned up its bucket data. The surviving replica re-sends the bucket from scratch, so stale
        // time-series segments reach the destination with a fresh receiver-local order in their change vector.
        //
        // The series must remain deleted after that resend. This verifies that the destination deleted-range
        // preserves predecessor version lineage, and that stale incoming segments are checked against deleted
        // ranges by version instead of by the flattened change-vector order.
        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Replication | RavenTestCategory.TimeSeries)]
        public async Task DeletedTimeSeriesShouldStayDeletedAfterBucketResendAfterMigrationSourceFailover()
        {
            var settings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Cluster.MoveToRehabGraceTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Cluster.StabilizationTime)] = "1",
            };

            var (nodes, leader) = await CreateRaftCluster(3, leaderIndex: 0, customSettings: settings);
            Assert.Equal("A", leader.ServerStore.NodeTag);

            var options = new Options
            {
                Server = leader,
                DatabaseMode = RavenDatabaseMode.Sharded,
                ModifyDatabaseRecord = record => record.Sharding = new ShardingConfiguration
                {
                    Orchestrator = new OrchestratorConfiguration
                    {
                        Topology = new OrchestratorTopology { Members = new List<string> { "A" } }
                    },
                    Shards = new Dictionary<int, DatabaseTopology>
                    {
                        { 0, new DatabaseTopology { Members = new List<string> { "B", "C" }, DynamicNodesDistribution = false } },
                        { 1, new DatabaseTopology { Members = new List<string> { "A" }, DynamicNodesDistribution = false } }
                    }
                }
            };

            using (var store = GetDocumentStore(options))
            {
                // a document whose bucket lives on shard 0 (the future migration source)
                string id = null;
                for (var i = 0; i < 5000; i++)
                {
                    var candidate = $"users/1$s{i}";
                    if (await Sharding.GetShardNumberForAsync(store, candidate) == 0)
                    {
                        id = candidate;
                        break;
                    }
                }
                Assert.NotNull(id);

                var baseline = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "owner" }, id);
                    var ts = session.TimeSeriesFor(id, "HeartRates");
                    for (var i = 1; i <= 10; i++)
                        ts.Append(baseline.AddMinutes(i), i);
                    await session.SaveChangesAsync();
                }

                // a deleted range authored on the source BEFORE the migration; after the migration it is
                // the only carrier of the source's version lineage among the destination's deleted ranges
                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor(id, "HeartRates").Delete(baseline.AddMinutes(1), baseline.AddMinutes(5));
                    await session.SaveChangesAsync();
                }

                var bucket = await Sharding.GetBucketAsync(store, id);
                var shard0Name = ShardHelper.ToShardName(store.Database, 0);
                var shard1Name = ShardHelper.ToShardName(store.Database, 1);

                var serverB = nodes.Single(s => s.ServerStore.NodeTag == "B");
                var serverC = nodes.Single(s => s.ServerStore.NodeTag == "C");

                var shard0OnB = ShardedDocumentDatabase.CastToShardedDocumentDatabase(
                    await serverB.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(shard0Name));
                var shard0OnC = ShardedDocumentDatabase.CastToShardedDocumentDatabase(
                    await serverC.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(shard0Name));

                foreach (var sourceShard in new[] { shard0OnB, shard0OnC })
                {
                    var hasSeries = await WaitForValueAsync(
                        () => Task.FromResult(CountTimeSeriesSegments(sourceShard, id) > 0 && CountDeletedRanges(sourceShard, id) > 0),
                        true, timeout: 30_000, interval: 333);
                    Assert.True(hasSeries,
                        $"the time series data did not reach shard 0 replica on node {sourceShard.ServerStore.NodeTag}");
                }

                // hold the source bucket cleanup on both source replicas, so the surviving replica still
                // has the stale bucket data when it takes over the migration (in production this is the
                // natural window between ownership transfer and cleanup)
                shard0OnB.ForTestingPurposesOnly().DelayDeleteBucket = new AsyncManualResetEvent();
                shard0OnC.ForTestingPurposesOnly().DelayDeleteBucket = new AsyncManualResetEvent();

                await Sharding.Resharding.StartMovingShardForId(store, id, toShard: 1, servers: nodes);

                var status = await WaitForValueAsync(async () =>
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    return record.Sharding.BucketMigrations.TryGetValue(bucket, out var migration)
                        ? migration.Status
                        : (MigrationStatus?)null;
                }, MigrationStatus.OwnershipTransferred, timeout: 60_000, interval: 333);
                Assert.Equal(MigrationStatus.OwnershipTransferred, status);

                var shard1OnA = ShardedDocumentDatabase.CastToShardedDocumentDatabase(
                    await leader.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(shard1Name));

                // the migrated time series arrived at the destination with composite (order|version) change vectors
                Assert.True(CountTimeSeriesSegments(shard1OnA, id) > 0);
                var migratedRangeChangeVector = GetDeletedRangeChangeVectors(shard1OnA, id).SingleOrDefault();
                Assert.NotNull(migratedRangeChangeVector);
                Assert.Contains("|", migratedRangeChangeVector);

                // the user deletes the entire series; the write is routed to the destination shard
                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor(id, "HeartRates").Delete();
                    await session.SaveChangesAsync();
                }

                Assert.Equal(0, CountTimeSeriesSegments(shard1OnA, id));
                using (var session = store.OpenAsyncSession())
                {
                    var values = await session.TimeSeriesFor(id, "HeartRates").GetAsync();
                    Assert.True(values == null || values.Length == 0);
                }

                // find which source replica owns the migration (it keeps the connection open for leftovers)
                RavenServer migrationOwner = null;
                var hasOwner = await WaitForValueAsync(() =>
                {
                    foreach (var (server, shardDb) in new[] { (serverB, shard0OnB), (serverC, shard0OnC) })
                    {
                        if (shardDb.ReplicationLoader.OutgoingHandlers.OfType<OutgoingMigrationReplicationHandler>().Any())
                        {
                            migrationOwner = server;
                            return Task.FromResult(true);
                        }
                    }
                    return Task.FromResult(false);
                }, true, timeout: 30_000, interval: 333);
                Assert.True(hasOwner, "no outgoing migration handler found on either source replica");

                var survivorShard0 = migrationOwner == serverB ? shard0OnC : shard0OnB;

                // the cleanup hold kept the stale bucket data (incl. the segment) on the survivor
                Assert.True(CountTimeSeriesSegments(survivorShard0, id) > 0);

                long survivorLastEtag;
                using (survivorShard0.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    survivorLastEtag = survivorShard0.DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction);
                }

                var preKillIncomingHandlers = shard1OnA.ReplicationLoader.IncomingHandlers
                    .OfType<IncomingMigrationReplicationHandler>().ToHashSet();

                // kill the migration owner; the surviving source replica takes over the migration task
                // and re-sends the whole bucket from scratch
                await DisposeServerAndWaitForFinishOfDisposalAsync(migrationOwner);

                var resendCompleted = await WaitForValueAsync(() =>
                {
                    foreach (var handler in shard1OnA.ReplicationLoader.IncomingHandlers.OfType<IncomingMigrationReplicationHandler>())
                    {
                        // a NEW incoming migration connection (from the surviving replica) that has
                        // processed everything the survivor holds
                        if (preKillIncomingHandlers.Contains(handler) == false &&
                            handler.LastDocumentEtag >= survivorLastEtag)
                            return Task.FromResult(true);
                    }
                    return Task.FromResult(false);
                }, true, timeout: 120_000, interval: 333);

                if (resendCompleted == false)
                {
                    var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                    var shard0Topology = record.Sharding.Shards[0];
                    var diagnostics = new System.Text.StringBuilder()
                        .AppendLine($"survivor: {survivorShard0.ServerStore.NodeTag}, survivorLastEtag: {survivorLastEtag}")
                        .AppendLine($"shard0 members: [{string.Join(",", shard0Topology.Members)}], rehabs: [{string.Join(",", shard0Topology.Rehabs)}]")
                        .AppendLine($"migration record present: {record.Sharding.BucketMigrations.ContainsKey(bucket)}" +
                                    (record.Sharding.BucketMigrations.TryGetValue(bucket, out var m) ? $", status: {m.Status}" : string.Empty))
                        .AppendLine($"survivor outgoing handlers: [{string.Join(",", survivorShard0.ReplicationLoader.OutgoingHandlers.Select(h => $"{h.GetType().Name}->{h.Destination?.Database}@{h.Destination?.Url}"))}]")
                        .AppendLine($"dest incoming handlers: [{string.Join(",", shard1OnA.ReplicationLoader.IncomingHandlers.Select(h => $"{h.GetType().Name} src={h.ConnectionInfo?.SourceDatabaseId} lastDocEtag={h.LastDocumentEtag}"))}]");
                    Assert.Fail("the surviving source replica did not re-send the bucket to the destination." + Environment.NewLine + diagnostics);
                }

                // the full-series delete is causally NEWER than the re-sent stale segment - the series must stay deleted
                Assert.Equal(0, CountTimeSeriesSegments(shard1OnA, id));
                using (var session = store.OpenAsyncSession())
                {
                    var values = await session.TimeSeriesFor(id, "HeartRates").GetAsync();
                    Assert.True(values == null || values.Length == 0,
                        $"the fully deleted time series was resurrected by the re-sent stale segment " +
                        $"(got {values?.Length} values back) - the local deleted-range change vector has no causal " +
                        "relation to the re-sent segment, or SegmentAlreadyDeleted failed to recognize the coverage");
                }
            }
        }

        private static int CountTimeSeriesSegments(ShardedDocumentDatabase shardDatabase, string docId)
        {
            using (shardDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                return shardDatabase.DocumentsStorage.TimeSeriesStorage.GetTimeSeriesFrom(context, 0, long.MaxValue)
                    .Count(s => s.DocId.ToString().Equals(docId, StringComparison.OrdinalIgnoreCase));
            }
        }

        private static int CountDeletedRanges(ShardedDocumentDatabase shardDatabase, string docId)
        {
            return GetDeletedRangeChangeVectors(shardDatabase, docId).Count;
        }

        private static List<string> GetDeletedRangeChangeVectors(ShardedDocumentDatabase shardDatabase, string docId)
        {
            var result = new List<string>();
            using (shardDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var deletedRange in shardDatabase.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesFrom(context, 0))
                {
                    TimeSeriesValuesSegment.ParseTimeSeriesKey(deletedRange.Key, context, out LazyStringValue rangeDocId, out _);
                    if (string.Equals(rangeDocId, docId, StringComparison.OrdinalIgnoreCase))
                        result.Add(deletedRange.ChangeVector);
                }
            }

            return result;
        }
    }
}
