using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.DocumentsCompression;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.Schemas;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Tables;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Cluster
{
    public class BucketStatsTests : ClusterTestBase
    {
        public BucketStatsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanGetBucketStats()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var bucket = await Sharding.GetBucketAsync(store, "users/1/$abc");

                var before = DateTime.UtcNow;
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        await session.StoreAsync(new Raven.Tests.Core.Utils.Entities.User(), $"users/{i}/$abc");
                    }

                    await session.SaveChangesAsync();
                }
                var after = DateTime.UtcNow;

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, 1));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(bucket, stats.Bucket);
                    Assert.Equal(2811, stats.Size);
                    Assert.Equal(10, stats.NumberOfDocuments);
                    Assert.True(stats.LastModified > before);
                    Assert.True(stats.LastModified < after);
                }

                using (var session = store.OpenAsyncSession())
                {
                    before = DateTime.UtcNow;
                    for (int i = 0; i < 10; i++)
                    {
                        if (i % 2 == 0)
                            continue;
                        var doc = await session.LoadAsync<Raven.Tests.Core.Utils.Entities.User>($"users/{i}/$abc");
                        doc.Age = i * 8;
                    }

                    await session.SaveChangesAsync();
                    after = DateTime.UtcNow;
                }

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(bucket, stats.Bucket);
                    Assert.Equal(2816, stats.Size);
                    Assert.Equal(10, stats.NumberOfDocuments);
                    Assert.True(stats.LastModified > before);
                    Assert.True(stats.LastModified < after);
                }

                using (var session = store.OpenAsyncSession())
                {
                    before = DateTime.UtcNow;
                    for (int i = 0; i < 10; i++)
                    {
                        if (i % 2 != 0)
                            continue;
                        session.Delete($"users/{i}/$abc");
                    }

                    await session.SaveChangesAsync();
                    after = DateTime.UtcNow;
                }

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(bucket, stats.Bucket);
                    Assert.Equal(1846, stats.Size);
                    Assert.Equal(5, stats.NumberOfDocuments);
                    Assert.True(stats.LastModified > before);
                    Assert.True(stats.LastModified < after);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanGetBucketStats2()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                var bucket1 = Sharding.GetBucket(record.Sharding, "users/1/$a");
                var bucket2 = Sharding.GetBucket(record.Sharding, "users/1/$b");
                var bucket3 = Sharding.GetBucket(record.Sharding, "users/1/$c");

                var buckets = new Dictionary<int, (int NumOfDocs, int Size)>
                {
                    [bucket1] = (10, 2771),
                    [bucket2] = (20, 5611),
                    [bucket3] = (30, 8431)
                };

                var before = DateTime.UtcNow;

                using (var session = store.OpenAsyncSession())
                {
                    var numOfDocs = buckets[bucket1].NumOfDocs;
                    for (int i = 0; i < numOfDocs; i++)
                    {
                        await session.StoreAsync(new Raven.Tests.Core.Utils.Entities.User(), $"users/{i}/$a");
                    }
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var numOfDocs = buckets[bucket2].NumOfDocs;
                    for (int i = 0; i < numOfDocs; i++)
                    {
                        await session.StoreAsync(new Raven.Tests.Core.Utils.Entities.User
                        {
                            Name = "b"
                        }, $"users/{i}/$b");
                    }
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var numOfDocs = buckets[bucket3].NumOfDocs;
                    for (int i = 0; i < numOfDocs; i++)
                    {
                        await session.StoreAsync(new Raven.Tests.Core.Utils.Entities.User
                        {
                            Name = "c",
                            Age = i
                        }, $"users/{i}/$c");
                    }

                    await session.SaveChangesAsync();
                }

                var after = DateTime.UtcNow;
                var shard = ShardHelper.GetShardNumberFor(record.Sharding, bucket1);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket1);
                    var expected = buckets[bucket1];

                    Assert.Equal(bucket1, stats.Bucket);
                    Assert.Equal(expected.Size, stats.Size);
                    Assert.Equal(expected.NumOfDocs, stats.NumberOfDocuments);
                    Assert.True(stats.LastModified > before);
                    Assert.True(stats.LastModified < after);
                }

                shard = ShardHelper.GetShardNumberFor(record.Sharding, bucket2);
                db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket2);
                    var expected = buckets[bucket2];

                    Assert.Equal(bucket2, stats.Bucket);
                    Assert.Equal(expected.Size, stats.Size);
                    Assert.Equal(expected.NumOfDocs, stats.NumberOfDocuments);
                    Assert.True(stats.LastModified > before);
                    Assert.True(stats.LastModified < after);
                }

                shard = ShardHelper.GetShardNumberFor(record.Sharding, bucket3);
                db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket3);
                    var expected = buckets[bucket3];

                    Assert.Equal(bucket3, stats.Bucket);
                    Assert.Equal(expected.Size, stats.Size);
                    Assert.Equal(expected.NumOfDocs, stats.NumberOfDocuments);
                    Assert.True(stats.LastModified > before);
                    Assert.True(stats.LastModified < after);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanGetBucketStatsForRange()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var numOfDocsPerBucket = 10;
                var bucketsInShard0 = new HashSet<int>();

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                for (int i = 1; i <= 100; i++)
                {
                    var suffix = i.ToString();
                    var bucket = Sharding.GetBucket(record.Sharding, $"users/1/${suffix}");
                    var shard = ShardHelper.GetShardNumberFor(record.Sharding, bucket);

                    if (shard != 0)
                        continue;

                    if (bucketsInShard0.Add(bucket) == false)
                        continue;

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int j = 0; j < numOfDocsPerBucket; j++)
                        {
                            await session.StoreAsync(new Raven.Tests.Core.Utils.Entities.User(), $"users/{j}/${suffix}");
                        }

                        await session.SaveChangesAsync();
                    }
                }

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard: 0));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatistics(ctx, fromBucket: 0, toBucket: int.MaxValue).ToList();
                    Assert.Equal(bucketsInShard0.Count, stats.Count);

                    foreach (var bucketStats in stats)
                    {
                        Assert.Equal(numOfDocsPerBucket, bucketStats.NumberOfDocuments);
                        Assert.True(bucketStats.Size > 0);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanGetBucketStats_WithDocumentExtensions()
        {
            const string id = "users/1$a";

            using (var store = Sharding.GetDocumentStore())
            {
                var bucket = await Sharding.GetBucketAsync(store, id);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "a"
                    }, id);
                    await session.SaveChangesAsync();
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shard = ShardHelper.GetShardNumberFor(record.Sharding, bucket);

                var expectedSize = 246;

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard));
                AssertStats(db, bucket, expectedSize);

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor(id).Increment("downloads", 10);
                    await session.SaveChangesAsync();

                    expectedSize = 459;
                }

                AssertStats(db, bucket, expectedSize);

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor(id, "heartrate").Append(DateTime.UtcNow, 123);
                    await session.SaveChangesAsync();

                    expectedSize = 654;
                }

                AssertStats(db, bucket, expectedSize);

                using (var session = store.OpenAsyncSession())
                await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    session.Advanced.Attachments.Store(id, "my_file", fileStream);
                    await session.SaveChangesAsync();

                    expectedSize = 944;
                }

                AssertStats(db, bucket, expectedSize);

                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false
                    }
                };

                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRevisionsOperation(configuration));

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.Name = "b";
                    await session.SaveChangesAsync();

                    expectedSize = 2470;
                }

                AssertStats(db, bucket, expectedSize);

                const string id2 = "users/2$a";
                const string id3 = "users/3$a";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "ab"
                    }, id2);
                    await session.StoreAsync(new User
                    {
                        Name = "cd"
                    }, id3);
                    await session.SaveChangesAsync();

                    expectedSize = 3536;
                }

                AssertStats(db, bucket, expectedSize, expectedDocs: 3);

                var dt = DateTime.Today.EnsureUtc();
                using (var session = store.OpenAsyncSession())
                {
                    var tsf = session.TimeSeriesFor(id2, "heartrate");

                    for (int i = 0; i < 100; i++)
                    {
                        tsf.Append(dt.AddMinutes(i), i);
                    }

                    await session.SaveChangesAsync();

                    expectedSize = 4822;
                }

                AssertStats(db, bucket, expectedSize, expectedDocs: 3);

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor(id2, "heartrate")
                        .Delete(dt.AddMinutes(10), dt.AddMinutes(20));

                    await session.SaveChangesAsync();

                    expectedSize = 4933;
                }

                AssertStats(db, bucket, expectedSize, expectedDocs: 3);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(id3);
                    await session.SaveChangesAsync();

                    expectedSize = 4917;
                }

                AssertStats(db, bucket, expectedSize, expectedDocs: 2);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(id2);
                    await session.SaveChangesAsync();

                    expectedSize = 4110;
                }

                AssertStats(db, bucket, expectedSize, expectedDocs: 1);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();

                    expectedSize = 3632;
                }

                AssertStats(db, bucket, expectedSize, expectedDocs: 0);

                await store.Maintenance.SendAsync(new DeleteRevisionsOperation(new DeleteRevisionsOperation.Parameters
                {
                    DocumentIds = new[] { id, id2, id3 }
                }));

                expectedSize = 1887;
                AssertStats(db, bucket, expectedSize, expectedDocs: 0);

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tombsCount = db.DocumentsStorage.GetNumberOfTombstones(ctx);
                    Assert.Equal(14, tombsCount);

                    await db.TombstoneCleaner.ExecuteCleanup();
                }

                expectedSize = 0;
                AssertStats(db, bucket, expectedSize, expectedDocs: 0);
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task BucketStatsShouldAccountAttachmentStreamSizePerBucket()
        {
            // different buckets, same shard
            const string id = "users/1";
            var attachment = new byte[10];

            using (var store = Sharding.GetDocumentStore())
            {
                var config = await Sharding.GetShardingConfigurationAsync(store);
                var bucket1 = Sharding.GetBucket(config, id);
                var shard = ShardHelper.GetShardNumberFor(config, bucket1);
                var diff = config.Shards.First(x => x.Key != shard).Key;

                var shardDatabase = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(ShardHelper.ToShardName(store.Database, shard));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "a" }, id);
                    await using (var fileStream = new MemoryStream(attachment))
                    {
                        session.Advanced.Attachments.Store(id, "attachment", fileStream);
                        await session.SaveChangesAsync();
                    }
                }
                //using (var session = store.OpenAsyncSession())
                //{
                //    await using (var fileStream = new MemoryStream(attachment))
                //    {
                //        session.Advanced.Attachments.Store(id, "ATTACHment", fileStream);
                //        await session.SaveChangesAsync();
                //    }
                //}

                await Sharding.Resharding.MoveShardForId(store, id);
                using (var session = store.OpenAsyncSession())
                {
                    await using (var fileStream = new MemoryStream(attachment))
                    {
                        session.Advanced.Attachments.Store(id, "ATTACHment", fileStream);
                        await session.SaveChangesAsync();
                    }
                }
                using (shardDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (var tx = ctx.OpenReadTransaction())
                {
                    var stats = shardDatabase.ShardedDocumentsStorage.AttachmentsStorage.GetStreamInfoForBucket(tx.InnerTransaction, bucket1);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task BucketStatsShouldTakeIntoAccountAttachmentStreamSize()
        {
            const string id = "users/1";

            using (var store = Sharding.GetDocumentStore())
            {
                var bucket = await Sharding.GetBucketAsync(store, id);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "a"
                    }, id);
                    await session.SaveChangesAsync();
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shard = ShardHelper.GetShardNumberFor(record.Sharding, bucket);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard));

                using (var session = store.OpenAsyncSession())
                {
                    var buffer = new byte[Constants.Size.Megabyte * 100];
                    var rand = new Random();
                    rand.NextBytes(buffer);

                    await using (var fileStream = new MemoryStream(buffer))
                    {
                        session.Advanced.Attachments.Store(id, "big_file", fileStream);
                        await session.SaveChangesAsync();
                    }
                }

                string hash = default;

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(104858139, stats.Size);
                    Assert.Equal(1, stats.NumberOfDocuments);

                    var attachmentMetadata = db.DocumentsStorage.AttachmentsStorage.GetAttachmentsMetadataForDocumentWithCounts(ctx, id).FirstOrDefault();
                    Assert.NotNull(attachmentMetadata);
                    Assert.Equal(1, attachmentMetadata.Count);
                    hash = attachmentMetadata.Hash;
                }

                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false
                    }
                };

                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRevisionsOperation(configuration));

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.Name = "bb";
                    await session.SaveChangesAsync();
                }

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                using (Slice.From(ctx.Allocator, hash, out var slice))
                {
                    var count = db.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, slice);
                    Assert.Equal(3, count.RegularHashes); // document attachment + 2 revision attachments

                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(104859331, stats.Size);
                    Assert.Equal(1, stats.NumberOfDocuments);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                using (Slice.From(ctx.Allocator, hash, out var slice))
                {
                    var count = db.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, slice);
                    Assert.Equal(2, count.RegularHashes); // 2 revision attachments

                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(104859146, stats.Size);
                    Assert.Equal(0, stats.NumberOfDocuments);
                }

                await store.Maintenance.SendAsync(new DeleteRevisionsOperation(new DeleteRevisionsOperation.Parameters
                {
                    DocumentIds = new[] { id }
                }));

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                using (Slice.From(ctx.Allocator, hash, out var slice))
                {
                    var count = db.DocumentsStorage.AttachmentsStorage.GetCountOfAttachmentsForHash(ctx, slice);
                    Assert.Equal(0, count.RegularHashes);

                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(857, stats.Size);
                    Assert.Equal(0, stats.NumberOfDocuments);
                }

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tombsCount = db.DocumentsStorage.GetNumberOfTombstones(ctx);
                    Assert.Equal(7, tombsCount);

                    await db.TombstoneCleaner.ExecuteCleanup();
                }

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Null(stats);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding, Skip = "RavenDB-19530 : bucket stats counts the RawSize of the value instead of the CompressedSize")]
        public async Task BucketStatsWithDocumentsCompression()
        {
            const string id = "companies/1";

            using (var store = Sharding.GetDocumentStore())
            {
                var bucket = await Sharding.GetBucketAsync(store, id);

                using (var session = store.OpenAsyncSession())
                {
                    var arr = new string[10_000];
                    for (int i = 0; i < arr.Length; i++)
                    {
                        arr[i] = "6";
                    }

                    await session.StoreAsync(new Company { Array = arr }, id);
                    await session.SaveChangesAsync();
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shard = ShardHelper.GetShardNumberFor(record.Sharding, bucket);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard));

                long originalSize;
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(bucket, stats.Bucket);
                    Assert.Equal(1, stats.NumberOfDocuments);

                    originalSize = stats.Size;
                }

                // turn on compression and modify the document
                var documentsCompression = new DocumentsCompressionConfiguration(true, true);
                await store.Maintenance.SendAsync(new UpdateDocumentsCompressionConfigurationOperation(documentsCompression));

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>(id);
                    company.Array[0] = "5";

                    await session.SaveChangesAsync();
                }

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                    Assert.Equal(1, stats.NumberOfDocuments);

                    Assert.True(stats.Size < originalSize);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanGetBucketStats_ManyDocs()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                await using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 100_000; i++)
                    {
                        var id = $"users/{i}";
                        await bulk.StoreAsync(new User(), id);
                    }
                }

                var total = 0L;
                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                foreach (var shardNumber in sharding.Shards.Keys)
                {
                    var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shardNumber));
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var numberOfDocsInShard = ShardedDocumentsStorage.GetBucketStatistics(ctx, fromBucket: 0, toBucket: int.MaxValue)
                            .Sum(s => s.NumberOfDocuments);

                        total += numberOfDocsInShard;
                    }
                }

                Assert.Equal(100_000, total);
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanGetBucketStatsForSampleData()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(DatabaseItemType.Documents | DatabaseItemType.Attachments));
                var total = 0L;
                long bucketsSize = 0;
                long docsSize = 0;

                var sharding = await Sharding.GetShardingConfigurationAsync(store);

                foreach (var shardNumber in sharding.Shards.Keys)
                {
                    var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shardNumber));
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var tableValuesSize = 0L;
                        var schema = db.DocumentsStorage.DocsSchema;
                        var table = new Table(schema, ctx.Transaction.InnerTransaction);
                        foreach (var result in table.SeekForwardFrom(schema.FixedSizeIndexes[Documents.AllDocsEtagsSlice], 0, 0))
                        {
                            tableValuesSize += result.Reader.Size;
                        }

                        schema = db.DocumentsStorage.AttachmentsStorage.AttachmentsSchema;
                        table = ctx.Transaction.InnerTransaction.OpenTable(schema, Attachments.AttachmentsMetadataSlice);

                        foreach (var result in table.SeekForwardFrom(schema.FixedSizeIndexes[Attachments.AttachmentsEtagSlice], 0, 0))
                        {
                            var attachment = AttachmentsStorage.TableValueToAttachment(ctx, ref result.Reader);
                            var size = attachment.Size;
                            tableValuesSize += result.Reader.Size;
                            tableValuesSize += size;
                        }

                        docsSize += tableValuesSize;
                    }
                }

                foreach (var shardNumber in sharding.Shards.Keys)
                {
                    var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shardNumber));
                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var bucketsStats = ShardedDocumentsStorage.GetBucketStatistics(ctx, 0, int.MaxValue).ToList();
                        var stats = bucketsStats.Sum(x => x.NumberOfDocuments);
                        total += stats;
                        bucketsSize += bucketsStats.Sum(x => x.Size);
                    }
                }
                Assert.Equal(1059, total);
                Assert.Equal(docsSize, bucketsSize);
            }
        }

        private static void AssertStats(DocumentDatabase db, int bucket, int expectedSize, int expectedDocs = 1)
        {
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                if (stats == null)
                {
                    Assert.Equal(expectedSize, 0);
                    Assert.Equal(expectedDocs, 0);
                    return;
                }

                Assert.Equal(expectedSize, stats.Size);
                Assert.Equal(expectedDocs, stats.NumberOfDocuments);
            }
        }

        private class Company
        {
            public string[] Array { get; set; }
        }
    }
}
