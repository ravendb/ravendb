using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Sharding;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Sharding.Cluster
{
    public class ReshardingTombstoneRepickLoop : ClusterTestBase
    {
        public ReshardingTombstoneRepickLoop(ITestOutputHelper output) : base(output)
        {
        }

        // Migrate a bucket that holds a live doc + pre-existing plain tombstones. Source cleanup must tag
        // those tombstones Artificial|FromResharding; otherwise the migrator re-picks the bucket forever
        // ("Only one bucket can be transferred at a time"). Regression guard for the re-pick loop.
        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task ExistingTombstones_ShouldBeMarkedArtificial_AndBucketNotRepicked()
        {
            using var store = Sharding.GetDocumentStore();

            const string suffix = "eu";

            // 5 docs in the same bucket (sticky suffix).
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User(), $"users/1${suffix}");
                await session.StoreAsync(new User(), $"users/2${suffix}");
                await session.StoreAsync(new User(), $"users/3${suffix}");
                await session.StoreAsync(new User(), $"users/4${suffix}");
                await session.StoreAsync(new User(), $"users/5${suffix}");
                await session.SaveChangesAsync();
            }

            // Delete 3 → 3 plain tombstones. users/1 and users/5 stay live docs.
            using (var session = store.OpenAsyncSession())
            {
                session.Delete($"users/2${suffix}");
                session.Delete($"users/3${suffix}");
                session.Delete($"users/4${suffix}");
                await session.SaveChangesAsync();
            }

            var id = $"users/1${suffix}";
            var oldLocation = await Sharding.GetShardNumberForAsync(store, id);
            var bucket = await Sharding.GetBucketAsync(store, id);

            var oldShard = (ShardedDocumentDatabase)await GetDocumentDatabaseInstanceFor(
                store, ShardHelper.ToShardName(store.Database, oldLocation));

            // Sanity: 3 plain tombstones, 0 artificial, before migration.
            var (plainBefore, artificialBefore) = CountTombstoneFlags(oldShard, bucket);
            Assert.Equal(3, plainBefore);
            Assert.Equal(0, artificialBefore);

            // Migrate the bucket away from oldLocation.
            await Sharding.Resharding.MoveShardForId(store, id);

            var recordAfterMigration = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            Assert.Empty(recordAfterMigration.Sharding.BucketMigrations);

            // ---- ASSERTION 1: the pre-existing tombstones must now be Artificial|FromResharding ----
            var (plainAfter, artificialAfter) = CountTombstoneFlags(oldShard, bucket);
            Assert.True(plainAfter == 0,
                $"{plainAfter} pre-existing tombstone(s) in bucket {bucket} were NOT marked Artificial|FromResharding " +
                $"after migration (artificial={artificialAfter}); they would keep the bucket eligible for re-migration forever.");

            // ---- ASSERTION 2: firing the auto-migrator must NOT re-pick the already-migrated bucket ----
            await oldShard.DocumentsMigrator.ExecuteMoveDocumentsAsync();

            var rePicked = await WaitForValueAsync(async () =>
            {
                var rec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                return rec.Sharding.BucketMigrations.ContainsKey(bucket);
            }, expectedVal: true, timeout: 5_000, interval: 100);
            Assert.False(rePicked,
                $"bucket {bucket} was RE-PICKED by the auto-migrator after it was already migrated and cleaned up - " +
                "the infinite re-pick loop that rejects concurrent migrations with 'Only one bucket can be transferred at a time'.");
        }

        // Same re-pick guard on a customer-like cluster: 4 nodes, 4 shards (RF=2), docs spread across all
        // shards (each bucket = live docs + pre-existing plain tombstones). Migrate one bucket, assert no re-pick.
        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task Mode3_MultiShard_CustomerLikeTopology_DoesNotRepickBucket()
        {
            var (nodes, leader) = await CreateRaftCluster(4, watcherCluster: true);
            var dbName = GetDatabaseName();

            // 4 shards, replication factor 2, spread across the 4 nodes — mirrors the customer's
            // shard layout (each shard lives on two nodes).
            var options = new Options
            {
                Server = leader,
                DatabaseMode = RavenDatabaseMode.Sharded,
                ReplicationFactor = 2,
                ModifyDatabaseRecord = r => r.Sharding = new ShardingConfiguration
                {
                    Orchestrator = new OrchestratorConfiguration
                    {
                        Topology = new OrchestratorTopology { Members = new List<string> { "A", "B", "C", "D" } }
                    },
                    Shards = new Dictionary<int, DatabaseTopology>
                    {
                        { 0, new DatabaseTopology { Members = new List<string> { "A", "B" } } },
                        { 1, new DatabaseTopology { Members = new List<string> { "B", "C" } } },
                        { 2, new DatabaseTopology { Members = new List<string> { "C", "D" } } },
                        { 3, new DatabaseTopology { Members = new List<string> { "D", "A" } } },
                    }
                }
            };

            using var store = GetDocumentStore(options, dbName);

            // Find a sticky suffix that lands on each of the 4 shards, so we can place a controlled
            // bucket (with live docs + tombstones) on every shard.
            var suffixPerShard = new Dictionary<int, string>();
            int probe = 0;
            while (suffixPerShard.Count < 4)
            {
                Assert.True(probe < 5000, "couldn't find a suffix for every shard");
                var suffix = $"s{probe++}";
                var shard = await Sharding.GetShardNumberForAsync(store, $"users/1${suffix}");
                suffixPerShard.TryAdd(shard, suffix);
            }

            // Spread docs across all shards: per shard, 8 docs in one bucket, then delete 5 of them
            // → each shard ends up with 3 live docs + 5 PLAIN tombstones in a single bucket.
            const int docsPerShard = 8;
            const int deletesPerShard = 5;
            var bucketPerShard = new Dictionary<int, int>();

            foreach (var (shard, suffix) in suffixPerShard)
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= docsPerShard; i++)
                        await session.StoreAsync(new User { Name = $"shard{shard}-{i}" }, $"users/{i}${suffix}");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= deletesPerShard; i++)
                        session.Delete($"users/{i}${suffix}");
                    await session.SaveChangesAsync();
                }

                bucketPerShard[shard] = await Sharding.GetBucketAsync(store, $"users/{deletesPerShard + 1}${suffix}");
            }

            // Pick shard 0 as the one we migrate away. Its bucket has live docs (to move) and
            // plain tombstones (to be left behind ungated → re-pick fuel).
            var srcShard = 0;
            var srcSuffix = suffixPerShard[srcShard];
            var srcBucket = bucketPerShard[srcShard];
            var migrateId = $"users/{deletesPerShard + 1}${srcSuffix}";

            // shard 0 lives on nodes A & B — grab the instance from A.
            var srcServer = nodes.Single(n => n.ServerStore.NodeTag == "A");
            var srcShardDb = (ShardedDocumentDatabase)await Databases.GetDocumentDatabaseInstanceFor(
                srcServer, store, ShardHelper.ToShardName(store.Database, srcShard));

            // Migrate the bucket off shard 0.
            await Sharding.Resharding.MoveShardForId(store, migrateId, servers: nodes);

            // ---- ASSERTION 1: pre-existing document tombstones on the source must be tagged ----
            var (plainAfter, artificialAfter) = CountTombstoneFlags(srcShardDb, srcBucket);
            Assert.True(plainAfter == 0,
                $"{plainAfter} pre-existing tombstone(s) in shard {srcShard} bucket {srcBucket} were NOT " +
                $"marked Artificial|FromResharding after migration (artificial={artificialAfter}).");

            // ---- ASSERTION 2: the auto-migrator must NOT re-pick the already-migrated bucket ----
            await srcShardDb.DocumentsMigrator.ExecuteMoveDocumentsAsync();

            var rePicked = await WaitForValueAsync(async () =>
            {
                var rec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                return rec.Sharding.BucketMigrations.ContainsKey(srcBucket);
            }, expectedVal: true, timeout: 5_000, interval: 100);

            Assert.False(rePicked,
                $"bucket {srcBucket} was RE-PICKED after it was already migrated and cleaned up - " +
                "the infinite re-pick loop that produces the customer's 'Only one bucket can be transferred at a time' error.");
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task UnusedDatabaseIds_DocumentCleanup_StripsOrphanDbId_EndToEnd()
        {
            // we flip ServerStore.Sharding.ManualMigration (a server-global flag) below, so don't share the server.
            DoNotReuseServer();

            using var store = Sharding.GetDocumentStore();
            const string id = "users/1$cleanup-doc";

            // Initial write -> bucket CV = shardDbId@N
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v1" }, id);
                await session.SaveChangesAsync();
            }

            var shardNumber = await Sharding.GetShardNumberForAsync(store, id);
            var bucket = await Sharding.GetBucketAsync(store, id);
            var shardDb = (ShardedDocumentDatabase)await GetDocumentDatabaseInstanceFor(
                store, ShardHelper.ToShardName(store.Database, shardNumber));
            var shardDbId = shardDb.DbBase64Id;

            // Disable the auto-migrator so it can't race our direct DeleteBucket invocation.
            shardDb.ServerStore.Sharding.ManualMigration = true;
            try
            {
                // Snapshot upTo = current bucket CV (= shardDbId@N).
                string snapshotUpTo;
                using (shardDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    snapshotUpTo = shardDb.ShardedDocumentsStorage
                        .GetMergedChangeVectorInBucket(context, bucket).AsString();
                }

                // Second write -> doc CV = shardDbId@(N+1), strictly ahead of snapshotUpTo (=shardDbId@N).
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.Name = "v2";
                    await session.SaveChangesAsync();
                }

                // Without UnusedDatabaseIds: the orphan etag makes the doc look ahead of upTo -> Skipped.
                shardDb.ShardedDocumentsStorage.UnusedDatabaseIds = null;
                var resultWithout = await RunDeleteBucketViaMergerAsync(shardDb, bucket, snapshotUpTo);
                Assert.Equal(ShardedDocumentDatabase.DeleteBucketCommand.DeleteBucketResult.Skipped, resultWithout);

                // With UnusedDatabaseIds = { shardDbId }: orphan stripped -> AlreadyMerged -> cleanup proceeds -> Empty.
                shardDb.ShardedDocumentsStorage.UnusedDatabaseIds = new HashSet<string> { shardDbId };
                var resultWith = await RunDeleteBucketViaMergerAsync(shardDb, bucket, snapshotUpTo);
                Assert.Equal(ShardedDocumentDatabase.DeleteBucketCommand.DeleteBucketResult.Empty, resultWith);
            }
            finally
            {
                shardDb.ShardedDocumentsStorage.UnusedDatabaseIds = null;
                shardDb.ServerStore.Sharding.ManualMigration = false;
            }
        }

        [RavenFact(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        public async Task UnusedDatabaseIds_TombstoneCleanup_StripsOrphanDbId_EndToEnd()
        {
            // we flip ServerStore.Sharding.ManualMigration (a server-global flag) below, so don't share the server.
            DoNotReuseServer();

            using var store = Sharding.GetDocumentStore();
            const string id = "users/1$cleanup-tomb";

            // Initial write -> bucket CV = shardDbId@N.
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v1" }, id);
                await session.SaveChangesAsync();
            }

            var shardNumber = await Sharding.GetShardNumberForAsync(store, id);
            var bucket = await Sharding.GetBucketAsync(store, id);
            var shardDb = (ShardedDocumentDatabase)await GetDocumentDatabaseInstanceFor(
                store, ShardHelper.ToShardName(store.Database, shardNumber));
            var shardDbId = shardDb.DbBase64Id;

            shardDb.ServerStore.Sharding.ManualMigration = true;
            try
            {
                // Snapshot upTo BEFORE the delete (= shardDbId@N).
                string snapshotUpTo;
                using (shardDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    snapshotUpTo = shardDb.ShardedDocumentsStorage
                        .GetMergedChangeVectorInBucket(context, bucket).AsString();
                }

                // Delete -> tombstone CV = shardDbId@(N+1), strictly ahead of snapshotUpTo.
                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                var (plainBefore, artificialBefore) = CountTombstoneFlags(shardDb, bucket);
                Assert.Equal(1, plainBefore);
                Assert.Equal(0, artificialBefore);

                // Without UnusedDatabaseIds: tombstone CV looks ahead of upTo -> not tagged (stays plain).
                shardDb.ShardedDocumentsStorage.UnusedDatabaseIds = null;
                await RunDeleteBucketViaMergerAsync(shardDb, bucket, snapshotUpTo);
                var (plainAfterWithout, artificialAfterWithout) = CountTombstoneFlags(shardDb, bucket);
                Assert.Equal(plainBefore, plainAfterWithout);
                Assert.Equal(artificialBefore, artificialAfterWithout);

                // With UnusedDatabaseIds = { shardDbId }: orphan stripped -> AlreadyMerged -> tagged Artificial|FromResharding.
                shardDb.ShardedDocumentsStorage.UnusedDatabaseIds = new HashSet<string> { shardDbId };
                await RunDeleteBucketViaMergerAsync(shardDb, bucket, snapshotUpTo);
                var (plainAfterWith, artificialAfterWith) = CountTombstoneFlags(shardDb, bucket);
                Assert.Equal(0, plainAfterWith);
                Assert.True(artificialAfterWith > artificialBefore,
                    $"expected the existing tombstone to be tagged Artificial|FromResharding, but artificial count did not increase ({artificialBefore} -> {artificialAfterWith}).");
            }
            finally
            {
                shardDb.ShardedDocumentsStorage.UnusedDatabaseIds = null;
                shardDb.ServerStore.Sharding.ManualMigration = false;
            }
        }

        // Runs the real DeleteBucket path via the TxMerger (as DeleteBucketAsync does), so the CV checks
        // inside DeleteBucket / MarkTombstonesAsArtificial are exercised end-to-end, not bypassed.
        private static async Task<ShardedDocumentDatabase.DeleteBucketCommand.DeleteBucketResult> RunDeleteBucketViaMergerAsync(
            ShardedDocumentDatabase shardDb, int bucket, string upToChangeVector)
        {
            var cmd = new ShardedDocumentDatabase.DeleteBucketCommand(shardDb, bucket, upToChangeVector);
            await shardDb.TxMerger.Enqueue(cmd);
            return cmd.Result;
        }

        // One case per extension CV check in HasDocumentExtensionWithGreaterChangeVector. Calls the private
        // method via reflection: in a full DeleteBucket the doc CV check fires first, and natural writes bump
        // doc + extension CV together, so the extension branch can't be isolated otherwise. Each case asserts
        // the branch returns true without UnusedDatabaseIds and false with it (i.e. Phase 4 wired the arg in).
        public enum ExtensionKind { Counter, TimeSeries, Attachment, Revision }

        [RavenTheory(RavenTestCategory.Cluster | RavenTestCategory.Sharding)]
        [InlineData(ExtensionKind.Counter)]
        [InlineData(ExtensionKind.TimeSeries)]
        [InlineData(ExtensionKind.Attachment)]
        [InlineData(ExtensionKind.Revision)]
        public async Task UnusedDatabaseIds_HasDocumentExtensionCheck_StripsOrphanDbId_PerExtension(ExtensionKind kind)
        {
            // we flip ServerStore.Sharding.ManualMigration (a server-global flag) below, so don't share the server.
            DoNotReuseServer();

            using var store = Sharding.GetDocumentStore();

            // revisions enabled for all cases (documents commonly have revisions, and revision-attachments etc. coexist).
            await store.Maintenance.ForDatabase(store.Database).SendAsync(
                new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = false }
                }));

            const string id = "users/1$ext-cleanup";

            // Step 1: write the bare doc. Bucket CV = shardDbId@N.
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "v1" }, id);
                await session.SaveChangesAsync();
            }

            var shardNumber = await Sharding.GetShardNumberForAsync(store, id);
            var bucket = await Sharding.GetBucketAsync(store, id);
            var shardDb = (ShardedDocumentDatabase)await GetDocumentDatabaseInstanceFor(
                store, ShardHelper.ToShardName(store.Database, shardNumber));
            var shardDbId = shardDb.DbBase64Id;

            shardDb.ServerStore.Sharding.ManualMigration = true;
            try
            {
                // Step 2: snapshot upTo = current bucket CV (= shardDbId@N), BEFORE writing the extension.
                string upToString;
                using (shardDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    upToString = shardDb.ShardedDocumentsStorage.GetMergedChangeVectorInBucket(context, bucket).AsString();
                }

                // Step 3: write the extension via normal session APIs -> its CV is now shardDbId@(N+1), ahead of upTo.
                MemoryStream attachmentStream = null;
                try
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        switch (kind)
                        {
                            case ExtensionKind.Counter:
                                session.CountersFor(id).Increment("downloads", 1);
                                break;
                            case ExtensionKind.TimeSeries:
                                session.TimeSeriesFor(id, "Heartrate").Append(DateTime.UtcNow, 60d);
                                break;
                            case ExtensionKind.Attachment:
                                attachmentStream = new MemoryStream(new byte[] { 1, 2, 3 });
                                session.Advanced.Attachments.Store(id, "file.bin", attachmentStream);
                                break;
                            case ExtensionKind.Revision:
                                var user = await session.LoadAsync<User>(id);
                                user.Name = "v2";
                                break;
                        }
                        await session.SaveChangesAsync();
                    }
                }
                finally
                {
                    attachmentStream?.Dispose();
                }

                // Step 4: invoke the private HasDocumentExtensionWithGreaterChangeVector directly (the method Phase 4 modified).
                var method = typeof(ShardedDocumentsStorage).GetMethod(
                    "HasDocumentExtensionWithGreaterChangeVector",
                    BindingFlags.NonPublic | BindingFlags.Instance);
                Assert.NotNull(method);

                using (shardDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var upTo = context.GetChangeVector(upToString);

                    // Without UnusedDatabaseIds: extension CV is ahead of upTo -> returns true (would block cleanup).
                    shardDb.ShardedDocumentsStorage.UnusedDatabaseIds = null;
                    var resultWithout = (bool)method.Invoke(shardDb.ShardedDocumentsStorage, new object[] { context, id, upTo });
                    Assert.True(resultWithout,
                        $"[{kind}] without UnusedDatabaseIds, HasDocumentExtensionWithGreaterChangeVector should return true (the {kind} CV is ahead of upTo).");

                    // With UnusedDatabaseIds = { shardDbId }: orphan stripped -> AlreadyMerged -> returns false.
                    shardDb.ShardedDocumentsStorage.UnusedDatabaseIds = new HashSet<string> { shardDbId };
                    var resultWith = (bool)method.Invoke(shardDb.ShardedDocumentsStorage, new object[] { context, id, upTo });
                    Assert.False(resultWith,
                        $"[{kind}] with UnusedDatabaseIds = {{ shardDbId }}, HasDocumentExtensionWithGreaterChangeVector should return false. " +
                        $"If this fails after a code change, suspect that the UnusedDatabaseIds arg was removed from the {kind} branch of HasDocumentExtensionWithGreaterChangeVector.");
                }
            }
            finally
            {
                shardDb.ShardedDocumentsStorage.UnusedDatabaseIds = null;
                shardDb.ServerStore.Sharding.ManualMigration = false;
            }
        }

        private static (int plain, int artificial) CountTombstoneFlags(ShardedDocumentDatabase shard, int bucket)
        {
            int plain = 0, artificial = 0;
            using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var tomb in shard.ShardedDocumentsStorage.RetrieveTombstonesByBucketFrom(context, bucket, 0))
                {
                    if (tomb.Flags.Contain(DocumentFlags.Artificial) && tomb.Flags.Contain(DocumentFlags.FromResharding))
                        artificial++;
                    else
                        plain++;
                }
            }
            return (plain, artificial);
        }
    }
}
