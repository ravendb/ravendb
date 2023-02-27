using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Schemas;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron.Data.Tables;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Cluster
{
    public class RavenDB_19614 : ClusterTestBase
    {
        public RavenDB_19614(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanGetMergedChangeVectorInBucketFromBucketStats_SingleBucket()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var id = "users/1/$abc";
                var bucket = await Sharding.GetBucketAsync(store, id);
                var shardNumber = await Sharding.GetShardNumberFor(store, id);

                // insert
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        await session.StoreAsync(new Raven.Tests.Core.Utils.Entities.User(), $"users/{i}/$abc");
                    }

                    await session.SaveChangesAsync();
                }

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shardNumber))
                    as ShardedDocumentDatabase;

                AssertMergedChangeVectorInBucket(db, bucket);

                // update
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        if (i % 2 == 0)
                            continue;
                        var doc = await session.LoadAsync<Raven.Tests.Core.Utils.Entities.User>($"users/{i}/$abc");
                        doc.Age = i * 8;
                    }

                    await session.SaveChangesAsync();
                }

                AssertMergedChangeVectorInBucket(db, bucket);

                // delete
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        if (i % 2 != 0)
                            continue;
                        session.Delete($"users/{i}/$abc");
                    }

                    await session.SaveChangesAsync();
                }

                AssertMergedChangeVectorInBucket(db, bucket);
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanGetMergedChangeVectorInBucketFromBucketStats_ManyBuckets()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                await using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 10_000; i++)
                    {
                        await bulk.StoreAsync(new Raven.Tests.Core.Utils.Entities.User
                        {
                            Name = i.ToString()
                        }, $"users/{i}");
                    }
                }

                await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store))
                {
                    using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var bucketsStats = ShardedDocumentsStorage.GetBucketStatistics(ctx, 0, int.MaxValue).ToList();
                        Assert.NotEmpty(bucketsStats);

                        foreach (var stats in bucketsStats)
                        {
                            AssertMergedChangeVectorInBucket(shard, stats.Bucket);
                        }
                    }
                }

            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanGetMergedChangeVectorInBucketFromBucketStats_WithDocumentExtensions()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var bucket = await Sharding.GetBucketAsync(store, "users/1/$abc");
                var shardNumber = await Sharding.GetShardNumberFor(store, "users/1/$abc");
                var shardName = ShardHelper.ToShardName(store.Database, shardNumber);
                var baseline = DateTime.UtcNow;

                await store.Maintenance.SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false
                    }
                }));

                // insert data
                for (int i = 0; i < 10; i++)
                {
                    var id = $"users/{i}/$abc";

                    using var session = store.OpenAsyncSession();
                    await session.StoreAsync(new User(), id);

                    var cf = session.CountersFor(id);
                    cf.Increment("likes", i);
                    cf.Increment("dislikes", -i);

                    var tsf1 = session.TimeSeriesFor(id, "heartrate");
                    var tsf2 = session.TimeSeriesFor(id, "blood-pressure");
                    for (int j = 0; j < 10; j++)
                    {
                        tsf1.Append(baseline.AddDays(j), j);
                        tsf2.Append(baseline.AddDays(j), j);
                    }

                    using var ms1 = new MemoryStream(new byte[] { 1, 2, 3, 4, 5, (byte)i });
                    using var ms2 = new MemoryStream(new byte[] { 6, 7, 8, 9, 10, (byte)i });
                    session.Advanced.Attachments.Store(id, $"image-{i}", ms1);
                    session.Advanced.Attachments.Store(id, $"file-{i}", ms2);

                    await session.SaveChangesAsync();
                }

                var db = await GetDocumentDatabaseInstanceFor(store, shardName) as ShardedDocumentDatabase;

                AssertMergedChangeVectorInBucket(db, bucket);

                // update docs
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var id = $"users/{i}/$abc";
                        var doc = await session.LoadAsync<User>(id);
                        doc.Name = $"Name-{i}";
                    }

                    await session.SaveChangesAsync();
                }

                AssertMergedChangeVectorInBucket(db, bucket);

                // update extensions
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var id = $"users/{i}/$abc";

                        session.CountersFor(id).Increment("likes", i);
                        session.TimeSeriesFor(id, "heartrate").Append(baseline.AddMinutes(10), i);
                    }

                    await session.SaveChangesAsync();
                }

                AssertMergedChangeVectorInBucket(db, bucket);

                // delete some extensions
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        if (i % 2 == 0)
                            continue;

                        var id = $"users/{i}/$abc";

                        session.CountersFor(id).Delete("dislikes");
                        session.TimeSeriesFor(id, "heartrate").Delete(baseline.AddDays(4), baseline.AddDays(8));
                        session.TimeSeriesFor(id, "blood-pressure").Delete();
                        session.Advanced.Attachments.Delete(id, $"file-{i}");
                    }

                    await session.SaveChangesAsync();
                }

                AssertMergedChangeVectorInBucket(db, bucket);

                // delete some docs
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        if (i % 2 != 0)
                            continue;

                        session.Delete($"users/{i}/$abc");
                    }

                    await session.SaveChangesAsync();
                }

                AssertMergedChangeVectorInBucket(db, bucket);
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanGetMergedChangeVectorInBucketFromBucketStats_InCluster()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 2, orchestratorReplicationFactor: 3);
            
            using (var store = Sharding.GetDocumentStore(options))
            {
                await using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        await bulk.StoreAsync(new User
                        {
                            Name = i.ToString()
                        }, $"users/{i}");
                    }
                }

                await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store))
                {
                    using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var bucketsStats = ShardedDocumentsStorage.GetBucketStatistics(ctx, 0, int.MaxValue).ToList();
                        Assert.NotEmpty(bucketsStats);

                        foreach (var stats in bucketsStats)
                        {
                            AssertMergedChangeVectorInBucket(shard, stats.Bucket);
                        }
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "RavenDB-19745 : Importing an attachment mess up the stream tag")]
        public async Task CanGetMergedChangeVectorInBucketFromBucketStats_UsingSampleData()
        {
            var t = GetTempFileName();
            using (var store2 = Sharding.GetDocumentStore())
            using (var store3 = Sharding.GetDocumentStore())
            {
                using (var ms = new MemoryStream(new byte[]{1, 2, 3, 4}))
                {
                    using (var session = store2.OpenSession())
                    {
                        session.Store(new(), "users/1");
                        session.Advanced.Attachments.Store("users/1", "a", ms);

                        session.SaveChanges();
                    }
                }

                var op = await store2.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), t);
                await op.WaitForCompletionAsync();

                op = await store3.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), t);
                await op.WaitForCompletionAsync();
            }

            using (var store = Sharding.GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.Documents
                                                                                | DatabaseItemType.Attachments 
                                                                                | DatabaseItemType.CounterGroups 
                                                                                | DatabaseItemType.RevisionDocuments 
                                                                                | DatabaseItemType.TimeSeries 
                                                                                | DatabaseItemType.Tombstones ));

                await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store))
                {
                    using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var bucketsStats = ShardedDocumentsStorage.GetBucketStatistics(ctx, 0, int.MaxValue).ToList();
                        Assert.NotEmpty(bucketsStats);

                        foreach (var stats in bucketsStats)
                        {
                            AssertMergedChangeVectorInBucket(shard, stats.Bucket);
                        }
                    }
                }
            }
        }

        private static ChangeVector CalculateMergedChangeVectorInBucket(DocumentDatabase database, DocumentsOperationContext context, int bucket)
        {
            var storage = database.DocumentsStorage;
            var merged = context.GetChangeVector(string.Empty);
            var table = new Table(storage.DocsSchema, context.Transaction.InnerTransaction);

            var index = storage.DocsSchema.DynamicKeyIndexes[Documents.AllDocsBucketAndEtagSlice];
            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, index, bucket, 0))
            {
                var documentCv = DocumentsStorage.TableValueToChangeVector(context, (int)Documents.DocumentsTable.ChangeVector, ref result.Result.Reader);
                merged = merged.MergeWith(documentCv, context);
            }

            index = storage.TombstonesSchema.DynamicKeyIndexes[Tombstones.TombstonesBucketAndEtagSlice];
            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, index, bucket, 0))
            {
                var tombstoneCv = DocumentsStorage.TableValueToChangeVector(context, (int)Tombstones.TombstoneTable.ChangeVector, ref result.Result.Reader);
                var flags = DocumentsStorage.TableValueToFlags((int)Tombstones.TombstoneTable.Flags, ref result.Result.Reader);
                if (flags.HasFlag(DocumentFlags.Artificial | DocumentFlags.FromResharding))
                    continue;

                merged = merged.MergeWith(tombstoneCv, context);
            }

            index = database.DocumentsStorage.CountersStorage.CountersSchema.DynamicKeyIndexes[Counters.CountersBucketAndEtagSlice];
            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, index, bucket, 0))
            {
                var counterCv = DocumentsStorage.TableValueToChangeVector(context, (int)Counters.CountersTable.ChangeVector, ref result.Result.Reader);
                merged = merged.MergeWith(counterCv, context);
            }

            index = database.DocumentsStorage.ConflictsStorage.ConflictsSchema.DynamicKeyIndexes[Conflicts.ConflictsBucketAndEtagSlice];
            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, index, bucket, 0))
            {
                var conflictCv = DocumentsStorage.TableValueToChangeVector(context, (int)Conflicts.ConflictsTable.ChangeVector, ref result.Result.Reader);
                merged = merged.MergeWith(conflictCv, context);
            }

            index = database.DocumentsStorage.RevisionsStorage.RevisionsSchema.DynamicKeyIndexes[Revisions.RevisionsBucketAndEtagSlice];
            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, index, bucket, 0))
            {
                var revisionCv = DocumentsStorage.TableValueToChangeVector(context, (int)Revisions.RevisionsTable.ChangeVector, ref result.Result.Reader);
                merged = merged.MergeWith(revisionCv, context);
            }

            index = database.DocumentsStorage.AttachmentsStorage.AttachmentsSchema.DynamicKeyIndexes[Attachments.AttachmentsBucketAndEtagSlice];
            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, index, bucket, 0))
            {
                var attachmentCv = DocumentsStorage.TableValueToChangeVector(context, (int)Attachments.AttachmentsTable.ChangeVector, ref result.Result.Reader);
                merged = merged.MergeWith(attachmentCv, context);
            }

            index = database.DocumentsStorage.TimeSeriesStorage.TimeSeriesSchema.DynamicKeyIndexes[
                Raven.Server.Documents.Schemas.TimeSeries.TimeSeriesBucketAndEtagSlice];
            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, index, bucket, 0))
            {
                var tsCv = DocumentsStorage.TableValueToChangeVector(context, (int)Raven.Server.Documents.Schemas.TimeSeries.TimeSeriesTable.ChangeVector, ref result.Result.Reader);
                merged = merged.MergeWith(tsCv, context);
            }

            index = database.DocumentsStorage.TimeSeriesStorage.DeleteRangesSchema.DynamicKeyIndexes[DeletedRanges.DeletedRangesBucketAndEtagSlice];
            foreach (var result in ShardedDocumentsStorage.GetItemsByBucket(context.Allocator, table, index, bucket, 0))
            {
                var deletedRangeCv = DocumentsStorage.TableValueToChangeVector(context, (int)DeletedRanges.DeletedRangeTable.ChangeVector, ref result.Result.Reader);
                merged = merged.MergeWith(deletedRangeCv, context);
            }

            return merged;
        }

        private static void AssertMergedChangeVectorInBucket(ShardedDocumentDatabase db, int bucket)
        {
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var expected = CalculateMergedChangeVectorInBucket(db, ctx, bucket);
                var fromStats = db.ShardedDocumentsStorage.GetMergedChangeVectorInBucket(ctx, bucket);

                Assert.Equal(expected.AsString(), fromStats.AsString());
            }
        }
    }
}
