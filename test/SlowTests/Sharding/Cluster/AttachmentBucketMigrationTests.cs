using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Sharding.Cluster
{
    public class AttachmentBucketMigrationTests : ClusterTestBase
    {
        public AttachmentBucketMigrationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Attachments)]
        public async Task PuttingAttachmentOverMigratedTombstoneShouldPreserveTombstoneChangeVector()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                const string id = "users/1";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "1" }, id);
                    await session.SaveChangesAsync();
                }

                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await store.Operations.SendAsync(new PutAttachmentOperation(id, "a1", stream, "a1/png"));
                }

                await store.Operations.SendAsync(new DeleteAttachmentOperation(id, "a1"));

                var bucket = await Sharding.GetBucketAsync(store, id);

                // bucket migration rewrites the attachment tombstone change vector into the composite 'order|version' shape
                await Sharding.Resharding.MoveShardForId(store, id);

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);
                var newShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, newLocation));
                var storage = (ShardedDocumentsStorage)newShard.DocumentsStorage;

                string tombstoneChangeVector = null;
                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    foreach (var tombstone in storage.RetrieveTombstonesByBucketFrom(context, bucket, 0))
                    {
                        if (tombstone.Type == Tombstone.TombstoneType.Attachment)
                            tombstoneChangeVector = tombstone.ChangeVector;
                    }
                }

                Assert.NotNull(tombstoneChangeVector);
                Assert.Contains("|", tombstoneChangeVector);

                AttachmentDetails putResult;
                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    putResult = await store.Operations.SendAsync(new PutAttachmentOperation(id, "a1", stream, "a1/png"));
                }

                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var status = ChangeVector.GetConflictStatus(context, tombstoneChangeVector, putResult.ChangeVector);
                    Assert.Equal(ConflictStatus.AlreadyMerged, status);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Attachments)]
        public async Task PutDirectOverMigratedTombstoneShouldPreserveTombstoneChangeVector()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                const string id = "users/1";
                const string name = "a1";
                const string contentType = "image/png";
                var attachmentBytes = new byte[] { 1, 2, 3 };

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "1" }, id);
                    await session.SaveChangesAsync();
                }

                AttachmentDetails putResult;
                using (var stream = new MemoryStream(attachmentBytes))
                {
                    putResult = await store.Operations.SendAsync(new PutAttachmentOperation(id, name, stream, contentType));
                }

                await store.Operations.SendAsync(new DeleteAttachmentOperation(id, name));

                var bucket = await Sharding.GetBucketAsync(store, id);
                await Sharding.Resharding.MoveShardForId(store, id);

                var newLocation = await Sharding.GetShardNumberForAsync(store, id);
                var newShard = ShardedDocumentDatabase.CastToShardedDocumentDatabase(
                    await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, newLocation)));

                var tombstone = GetAttachmentTombstone(newShard, bucket);
                Assert.NotNull(tombstone.ChangeVector);
                Assert.NotNull(tombstone.Key);
                Assert.Contains("|", tombstone.ChangeVector);

                var storage = newShard.DocumentsStorage;
                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                using (DocumentIdWorker.GetLowerIdSliceAndStorageKeyForBackwardCompatibility(context, name, out _, out Slice nameSlice))
                using (DocumentIdWorker.GetLowerIdSliceAndStorageKeyForBackwardCompatibility(context, contentType, out _, out Slice contentTypeSlice))
                using (Slice.From(context.Allocator, putResult.Hash, out Slice base64Hash))
                using (Slice.From(context.Allocator, tombstone.Key, out Slice keySlice))
                using (var stream = new MemoryStream(attachmentBytes))
                {
                    storage.AttachmentsStorage.PutAttachmentStream(context, keySlice, base64Hash, stream);
                    storage.AttachmentsStorage.PutDirect(context, keySlice, nameSlice, contentTypeSlice, base64Hash, remoteParams: null, attachmentBytes.Length, isRevision: false);

                    tx.Commit();
                }

                Assert.Null(GetAttachmentTombstoneChangeVector(newShard, bucket));

                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var attachment = storage.AttachmentsStorage.GetAttachment(context, id, name, AttachmentType.Document, null,
                        putResult.Hash, contentType, usePartialKey: false);
                    Assert.NotNull(attachment);

                    var status = ChangeVector.GetConflictStatus(context, tombstone.ChangeVector, attachment.ChangeVector);
                    Assert.Equal(ConflictStatus.AlreadyMerged, status);
                }
            }
        }

        // Regression setup: after bucket ownership is transferred, source cleanup can still be pending.
        // If the migration owner dies in that window, another source replica can take over and resend
        // stale bucket data from scratch.
        //
        // Migration replication rewrites the receiver-local 'order' but keeps the original 'version'
        // lineage. Therefore, when the destination re-creates an attachment over a migrated tombstone,
        // the new attachment CV must inherit the tombstone's version lineage; otherwise a re-sent stale
        // tombstone compares as Conflict and can delete the newer write.
        //
        // Relevant internals:
        // - IncomingMigrationReplicationHandler.SaveSourceEtag intentionally does not checkpoint.
        // - IncomingMigrationReplicationHandler.PreProcessItem keeps version and assigns new order.
        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Replication | RavenTestCategory.Attachments)]
        public async Task ReputAttachmentShouldSurviveBucketResendAfterMigrationSourceFailover()
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

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "owner" }, id);
                    await session.SaveChangesAsync();
                }

                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await store.Operations.SendAsync(new PutAttachmentOperation(id, "a1", stream, "image/png"));
                }

                await store.Operations.SendAsync(new DeleteAttachmentOperation(id, "a1"));

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
                    var hasTombstone = await WaitForValueAsync(
                        () => Task.FromResult(GetAttachmentTombstoneChangeVector(sourceShard, bucket) != null),
                        true, timeout: 30_000, interval: 333);
                    Assert.True(hasTombstone,
                        $"the attachment tombstone did not reach shard 0 replica on node {sourceShard.ServerStore.NodeTag}");
                }

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

                var migratedTombstoneChangeVector = GetAttachmentTombstoneChangeVector(shard1OnA, bucket);
                Assert.NotNull(migratedTombstoneChangeVector);
                Assert.Contains("|", migratedTombstoneChangeVector);

                using (var stream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    await store.Operations.SendAsync(new PutAttachmentOperation(id, "a1", stream, "image/png"));
                }

                Assert.Null(GetAttachmentTombstoneChangeVector(shard1OnA, bucket));
                using (var session = store.OpenAsyncSession())
                {
                    Assert.True(await session.Advanced.Attachments.ExistsAsync(id, "a1"));
                }

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

                Assert.NotNull(GetAttachmentTombstoneChangeVector(survivorShard0, bucket));

                long survivorLastEtag;
                using (survivorShard0.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    survivorLastEtag = survivorShard0.DocumentsStorage.ReadLastEtag(context.Transaction.InnerTransaction);
                }

                var preKillIncomingHandlers = shard1OnA.ReplicationLoader.IncomingHandlers
                    .OfType<IncomingMigrationReplicationHandler>().ToHashSet();

                await DisposeServerAndWaitForFinishOfDisposalAsync(migrationOwner);

                var resendCompleted = await WaitForValueAsync(() =>
                {
                    foreach (var handler in shard1OnA.ReplicationLoader.IncomingHandlers.OfType<IncomingMigrationReplicationHandler>())
                    {
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

                using (var session = store.OpenAsyncSession())
                {
                    Assert.True(await session.Advanced.Attachments.ExistsAsync(id, "a1"),
                        "the re-created attachment was deleted by the re-sent stale attachment tombstone - " +
                        "its change vector did not preserve the version lineage of the tombstone it replaced");

                    var attachment = await session.Advanced.Attachments.GetAsync(id, "a1");
                    Assert.NotNull(attachment);
                    Assert.Equal(new byte[] { 1, 2, 3 }, await ReadAll(attachment.Stream));
                }
            }
        }

        private static string GetAttachmentTombstoneChangeVector(ShardedDocumentDatabase shardDatabase, int bucket)
        {
            return GetAttachmentTombstone(shardDatabase, bucket).ChangeVector;
        }

        private static (string ChangeVector, string Key) GetAttachmentTombstone(ShardedDocumentDatabase shardDatabase, int bucket)
        {
            var storage = (ShardedDocumentsStorage)shardDatabase.DocumentsStorage;
            using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var tombstone in storage.RetrieveTombstonesByBucketFrom(context, bucket, 0))
                {
                    if (tombstone.Type == Tombstone.TombstoneType.Attachment)
                        return (tombstone.ChangeVector, tombstone.LowerId.ToString());
                }
            }

            return (null, null);
        }

        private static async Task<byte[]> ReadAll(Stream stream)
        {
            using (var ms = new MemoryStream())
            {
                await stream.CopyToAsync(ms);
                return ms.ToArray();
            }
        }
    }
}
