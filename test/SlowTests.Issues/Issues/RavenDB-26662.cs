using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using Raven.Server.Commercial.WriteUsageMetering;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Server;
using Tests.Infrastructure;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Issues
{
    public class RavenDB_26662 : ClusterTestBase
    {
        public RavenDB_26662(ITestOutputHelper output) : base(output)
        {
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.SupervisorSamplePeriod)] = "50";
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.WorkerSamplePeriod)] = "25";
            DefaultClusterSettings[RavenConfiguration.GetKey(x => x.Cluster.OnErrorDelayTime)] = "15";
        }

        private sealed class Item
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private static async Task<string> ReadLiveChangeVectorAsync(List<RavenServer> nodes, string nodeTag, string resourceName)
        {
            var node = nodes.Single(n => n.ServerStore.NodeTag == nodeTag);
            var db = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(resourceName);
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                return DocumentsStorage.GetDatabaseChangeVector(context);
            }
        }

        private static async Task<string> ReadLiveMergedChangeVectorAsync(List<RavenServer> nodes, List<string> members, string resourceName)
        {
            var changeVectors = new List<string>();
            foreach (var member in members)
                changeVectors.Add(await ReadLiveChangeVectorAsync(nodes, member, resourceName));

            return Normalize(ChangeVectorUtils.MergeVectors(changeVectors));
        }

        private static async Task<string> ReadDbIdAsync(List<RavenServer> nodes, string nodeTag, string resourceName)
        {
            var node = nodes.Single(n => n.ServerStore.NodeTag == nodeTag);
            var db = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(resourceName);
            return db.DbBase64Id;
        }

        private static WriteUsageDatabaseSnapshot SnapshotEntry(RavenServer leader, string topologyId)
            => leader.ServerStore.Observer.LatestWriteUsageSnapshot?.Databases.SingleOrDefault(d => d.TopologyId == topologyId);

        private static string Normalize(string changeVector)
            => string.IsNullOrEmpty(changeVector) ? string.Empty : ChangeVectorUtils.MergeVectors(changeVector);

        private static string SnapshotCvFor(RavenServer leader, string topologyId)
        {
            var entry = SnapshotEntry(leader, topologyId);
            return entry == null ? null : Normalize(entry.ChangeVector);
        }

        private static async Task WriteDocsAsync(IDocumentStore store, int count, int replicas = 2)
        {
            using var session = store.OpenAsyncSession();
            session.Advanced.WaitForReplicationAfterSaveChanges(replicas: replicas);
            for (var i = 0; i < count; i++)
                await session.StoreAsync(new Item { Name = $"item-{i}" }, $"items/{i}");
            await session.SaveChangesAsync();
        }

        [RavenFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster)]
        public async Task SpecialTags_AreStripped_FromMergedSnapshotChangeVector()
        {
            // A member's DatabaseChangeVector can carry a MOVE (resharding) entry, which is a migration index
            // rather than a per-write counter; the migrated documents are already counted via their preserved
            // node-tag entries, so MOVE must be stripped before merging to avoid double-counting. All other
            // entries - RAFT (cluster transactions), TRXN, SINK, and real node tags - are counted and kept.
            var nodeAId = Guid.NewGuid().ToBase64Unpadded();
            var nodeBId = Guid.NewGuid().ToBase64Unpadded();
            var raftId = Guid.NewGuid().ToBase64Unpadded();
            var moveId = Guid.NewGuid().ToBase64Unpadded();
            var trxnId = Guid.NewGuid().ToBase64Unpadded();
            var sinkId = Guid.NewGuid().ToBase64Unpadded();

            // Two members, each reporting their own node entry mixed with special tags.
            var memberReports = new[]
            {
                $"A:7-{nodeAId}, RAFT:3-{raftId}, MOVE:2-{moveId}",
                $"B:9-{nodeBId}, TRXN:4-{trxnId}, SINK:1-{sinkId}",
            };

            using var store = GetDocumentStore();
            var database = await GetDatabase(store.Database);

            // Mirror the snapshot collection in ClusterObserver: strip the MOVE tag per member, then merge.
            var memberChangeVectors = new List<string>();
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                foreach (var report in memberReports)
                    memberChangeVectors.Add(ChangeVector.StripMoveTag(report, context).AsString());
            }

            var merged = ChangeVectorUtils.MergeVectors(memberChangeVectors);

            // Only MOVE is stripped from the final merged change vector that goes into the payload.
            Assert.DoesNotContain("MOVE", merged);

            // RAFT, TRXN, and SINK are kept so they are counted as write-usage.
            Assert.Contains("RAFT", merged);
            Assert.Contains("TRXN", merged);
            Assert.Contains("SINK", merged);

            // The real node entries (and their database ids) survive the strip+merge.
            Assert.Contains(nodeAId, merged);
            Assert.Contains(nodeBId, merged);
            Assert.Contains("A:7", merged);
            Assert.Contains("B:9", merged);

            // Every entry except the single MOVE remains: A, B, RAFT, TRXN, SINK => 5 entries.
            Assert.Equal(5, merged.ToChangeVectorList().Count);
        }

        [RavenFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster)]
        public async Task ChangeVector_IsMergedOverMembers()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

            using (var store = GetDocumentStore(new Options
            {
                ReplicationFactor = 3,
                Server = leader
            }))
            {
                await WriteDocsAsync(store, count: 50);

                var record = GetDatabaseRecord(store);
                var topologyId = record.Topology.DatabaseTopologyIdBase64;
                var members = record.Topology.Members;
                Assert.Equal(3, members.Count);

                var expected = await ReadLiveMergedChangeVectorAsync(nodes, members, store.Database);
                Assert.False(string.IsNullOrEmpty(expected), "Expected writes to populate the change vector.");

                // Wait for the observer to converge to that merged change vector.
                var converged = await WaitForValueAsync(() => SnapshotCvFor(leader, topologyId), expected, timeout: 30_000, interval: 100);

                // Freeze ticks for a deterministic read of the published snapshot.
                leader.ServerStore.Observer.Suspended = true;

                var dbEntry = SnapshotEntry(leader, topologyId);
                Assert.NotNull(dbEntry);

                // (1) The snapshot reports a single merged change vector per topology - no per-node array.
                Assert.Equal(expected, converged);
                Assert.Equal(expected, Normalize(dbEntry.ChangeVector));

                // (2) It is a MERGE across all members: the merged change vector spans every member's
                // database id, not just one node's contribution.
                foreach (var member in members)
                {
                    var dbId = await ReadDbIdAsync(nodes, member, store.Database);
                    Assert.Contains(dbId, dbEntry.ChangeVector);
                }
            }
        }

        [RavenFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster)]
        public async Task EachDatabase_ProducesOneEntry_WithDistinctTopologyId()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

            var stores = new List<DocumentStore>();
            try
            {
                // Three databases with different write volumes => different, independently verified change vectors.
                var counts = new[] { 10, 25, 40 };
                foreach (var c in counts)
                {
                    var store = GetDocumentStore(new Options { ReplicationFactor = 3, Server = leader });
                    stores.Add(store);
                    await WriteDocsAsync(store, count: c);
                }

                var topologyIds = new List<string>();
                var expectedCvById = new Dictionary<string, string>();
                foreach (var store in stores)
                {
                    var record = GetDatabaseRecord(store);
                    var topologyId = record.Topology.DatabaseTopologyIdBase64;
                    topologyIds.Add(topologyId);
                    var expected = await ReadLiveMergedChangeVectorAsync(nodes, record.Topology.Members, store.Database);
                    expectedCvById[topologyId] = expected;

                    await WaitForValueAsync(() => SnapshotCvFor(leader, topologyId), expected, timeout: 30_000, interval: 100);
                }

                leader.ServerStore.Observer.Suspended = true;

                // Distinct topology ids, one per database.
                Assert.Equal(3, topologyIds.Distinct().Count());

                foreach (var topologyId in topologyIds)
                {
                    var entries = leader.ServerStore.Observer.LatestWriteUsageSnapshot.Databases
                        .Where(d => d.TopologyId == topologyId)
                        .ToList();

                    Assert.Single(entries);
                    Assert.Equal(expectedCvById[topologyId], Normalize(entries[0].ChangeVector));
                }
            }
            finally
            {
                foreach (var store in stores)
                    store.Dispose();
            }
        }

        [RavenFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task ShardedDatabase_ProducesOneEntryPerShard_WithoutOrchestrator()
        {
            const int shards = 3;
            var database = GetDatabaseName();
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            await ShardingCluster.CreateShardedDatabaseInCluster(database, replicationFactor: 2, (nodes, leader), shards: shards);

            using (var store = new DocumentStore { Database = database, Urls = new[] { leader.WebUrl } })
            {
                store.Initialize();
                await WriteDocsAsync(store, count: 30, replicas: 1);

                var shardTopologies = await ShardingCluster.GetShards(store);
                Assert.Equal(shards, shardTopologies.Count);

                var shardingConfig = await Sharding.GetShardingConfigurationAsync(store);
                var orchestratorTopologyId = shardingConfig.Orchestrator.Topology.DatabaseTopologyIdBase64;

                // Per-shard merged change vector, computed live from each shard's members.
                var expectedCvByShardTopologyId = new Dictionary<string, string>();
                foreach (var (shardNumber, topology) in shardTopologies)
                {
                    var shardName = ShardHelper.ToShardName(database, shardNumber);
                    var expected = await ReadLiveMergedChangeVectorAsync(nodes, topology.Members, shardName);
                    expectedCvByShardTopologyId[topology.DatabaseTopologyIdBase64] = expected;

                    await WaitForValueAsync(() => SnapshotCvFor(leader, topology.DatabaseTopologyIdBase64), expected, timeout: 30_000, interval: 100);
                }

                leader.ServerStore.Observer.Suspended = true;

                // Exactly one entry per shard topology, each with its own merged change vector.
                foreach (var (shardTopologyId, expected) in expectedCvByShardTopologyId)
                {
                    var entries = leader.ServerStore.Observer.LatestWriteUsageSnapshot.Databases
                        .Where(d => d.TopologyId == shardTopologyId)
                        .ToList();

                    Assert.Single(entries);
                    Assert.Equal(expected, Normalize(entries[0].ChangeVector));
                }

                // The orchestrator topology is NOT a data-bearing topology => it must not produce an entry.
                if (string.IsNullOrEmpty(orchestratorTopologyId) == false)
                {
                    Assert.DoesNotContain(leader.ServerStore.Observer.LatestWriteUsageSnapshot.Databases,
                        d => d.TopologyId == orchestratorTopologyId);
                }
            }
        }

        [RavenFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster)]
        public async Task NewDatabase_NoWrites_HasEntryWithEmptyChangeVector()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

            using (var store = GetDocumentStore(new Options { ReplicationFactor = 3, Server = leader }))
            {
                var record = GetDatabaseRecord(store);
                var topologyId = record.Topology.DatabaseTopologyIdBase64;

                // No documents were written; the merged change vector over members is empty.
                var expected = await ReadLiveMergedChangeVectorAsync(nodes, record.Topology.Members, store.Database);
                Assert.Equal(string.Empty, expected);

                // The entry is still produced for a brand-new database; wait for it to appear.
                await WaitForValueAsync(() => SnapshotEntry(leader, topologyId) != null, true, timeout: 30_000, interval: 100);

                leader.ServerStore.Observer.Suspended = true;

                var dbEntry = SnapshotEntry(leader, topologyId);
                Assert.NotNull(dbEntry);
                Assert.True(string.IsNullOrEmpty(dbEntry.ChangeVector),
                    $"Expected an empty change vector for a brand-new database, got '{dbEntry.ChangeVector}'.");
            }
        }

        [RavenFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster)]
        public async Task TopologyDatabaseId_IsStableAcrossTicks_AndChangesOnRecreate()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);
            var database = GetDatabaseName();

            using (var store = new DocumentStore { Database = database, Urls = new[] { leader.WebUrl } })
            {
                store.Initialize();
                await CreateDatabaseInCluster(database, replicationFactor: 3, leader.WebUrl);

                var firstId = GetDatabaseRecord(store).Topology.DatabaseTopologyIdBase64;

                // Stable across two distinct observer ticks: read the id the snapshot reports, let the
                // iteration advance, read it again.
                await WaitForValueAsync(() => SnapshotEntry(leader, firstId) != null, true, timeout: 30_000, interval: 100);
                var idTickA = SnapshotEntry(leader, firstId).TopologyId;

                var iterationA = leader.ServerStore.Observer._iteration;
                await WaitForValueAsync(() => leader.ServerStore.Observer._iteration >= iterationA + 2, true, timeout: 30_000, interval: 100);

                var entryTickB = SnapshotEntry(leader, firstId);
                Assert.NotNull(entryTickB);
                var idTickB = entryTickB.TopologyId;

                Assert.Equal(firstId, idTickA);
                Assert.Equal(idTickA, idTickB);

                // Delete and recreate the SAME name => a fresh topology id.
                await store.Maintenance.Server.SendAsync(new DeleteDatabasesOperation(database, hardDelete: true));
                await WaitForValueAsync(() => SnapshotEntry(leader, firstId) != null, false, timeout: 30_000, interval: 100);

                await CreateDatabaseInCluster(database, replicationFactor: 3, leader.WebUrl);
                var secondId = GetDatabaseRecord(store).Topology.DatabaseTopologyIdBase64;

                Assert.NotEqual(firstId, secondId);

                await WaitForValueAsync(() => SnapshotEntry(leader, secondId) != null, true, timeout: 30_000, interval: 100);
                Assert.Null(SnapshotEntry(leader, firstId));
            }
        }

        [RavenFact(RavenTestCategory.Licensing | RavenTestCategory.Cluster)]
        public async Task DownMember_MovesToRehab_DoesNotSkewOrBreakTheMergedChangeVector()
        {
            var (nodes, leader) = await CreateRaftCluster(3, watcherCluster: true);

            using (var store = GetDocumentStore(new Options { ReplicationFactor = 3, Server = leader }))
            {
                await WriteDocsAsync(store, count: 50);

                var record = GetDatabaseRecord(store);
                var topologyId = record.Topology.DatabaseTopologyIdBase64;
                Assert.Equal(3, record.Topology.Members.Count);

                // Take down a follower member; it will move to Rehab and leave Members.
                var victim = nodes.First(n => n.ServerStore.NodeTag != leader.ServerStore.NodeTag
                                              && record.Topology.Members.Contains(n.ServerStore.NodeTag));
                var victimTag = victim.ServerStore.NodeTag;
                await DisposeServerAndWaitForFinishOfDisposalAsync(victim);
                var remaining = nodes.Where(n => n.ServerStore.NodeTag != victimTag).ToList();

                // Wait until the topology reflects the rehab move (Members drops to the 2 healthy nodes).
                await WaitForValueAsync(() =>
                {
                    var t = GetDatabaseRecord(store).Topology;
                    return t.Members.Count == 2 && t.Members.Contains(victimTag) == false;
                }, true, timeout: 30_000, interval: 200);

                var healthyMembers = GetDatabaseRecord(store).Topology.Members;
                Assert.Equal(2, healthyMembers.Count);

                // The merged change vector over the healthy members only, computed live.
                var expected = await ReadLiveMergedChangeVectorAsync(remaining, healthyMembers, store.Database);
                Assert.False(string.IsNullOrEmpty(expected), "Healthy members should still hold the written data.");

                // The snapshot keeps reflecting the healthy members' merged change vector - the down node
                // neither zeroes it out nor skews it (validates Members-only).
                await WaitForValueAsync(() => SnapshotCvFor(leader, topologyId), expected, timeout: 30_000, interval: 100);

                leader.ServerStore.Observer.Suspended = true;

                var dbEntry = SnapshotEntry(leader, topologyId);
                Assert.NotNull(dbEntry);
                Assert.Equal(expected, Normalize(dbEntry.ChangeVector));
            }
        }
    }
}
