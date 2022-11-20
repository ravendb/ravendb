using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Cluster
{
    public class BucketStats : ClusterTestBase
    {
        public BucketStats(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanGetBucketStats()
        {
            var bucket = ShardHelper.GetBucket("users/1/$abc");

            using (var store = Sharding.GetDocumentStore())
            {
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
            var bucket1 = ShardHelper.GetBucket("users/1/$a");
            var bucket2 = ShardHelper.GetBucket("users/1/$b");
            var bucket3 = ShardHelper.GetBucket("users/1/$c");

            var buckets = new Dictionary<int, (int NumOfDocs, int Size)>
            {
                [bucket1] = (10, 2771), 
                [bucket2] = (20, 5611), 
                [bucket3] = (30, 8431)
            };

            using (var store = Sharding.GetDocumentStore())
            {
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
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shard = ShardHelper.GetShardNumber(record.Sharding.BucketRanges, bucket1);

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

                shard = ShardHelper.GetShardNumber(record.Sharding.BucketRanges, bucket2);
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

                shard = ShardHelper.GetShardNumber(record.Sharding.BucketRanges, bucket3);
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
                    var bucket = ShardHelper.GetBucket($"users/1/${suffix}");
                    var shard = ShardHelper.GetShardNumber(record.Sharding.BucketRanges, bucket);

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
            var bucket = ShardHelper.GetBucket(id);

            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "a"
                    }, id);
                    await session.SaveChangesAsync();
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shard = ShardHelper.GetShardNumber(record.Sharding.BucketRanges, bucket);

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

                    expectedSize = 939;
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

                    expectedSize = 2469;
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

                    expectedSize = 3535;
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

                    expectedSize = 4821;
                }

                AssertStats(db, bucket, expectedSize, expectedDocs: 3);

                using (var session = store.OpenAsyncSession())
                {
                    session.TimeSeriesFor(id2, "heartrate")
                        .Delete(dt.AddMinutes(10), dt.AddMinutes(20));

                    await session.SaveChangesAsync();

                    expectedSize = 4932;
                }

                AssertStats(db, bucket, expectedSize, expectedDocs: 3);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(id3);
                    await session.SaveChangesAsync();

                    expectedSize = 4916;
                }

                AssertStats(db, bucket, expectedSize, expectedDocs: 2);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(id2);
                    await session.SaveChangesAsync();

                    expectedSize = 4013;
                }

                AssertStats(db, bucket, expectedSize, expectedDocs: 1);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();

                    expectedSize = 3305;
                }

                AssertStats(db, bucket, expectedSize, expectedDocs: 0);

                await store.Maintenance.SendAsync(new DeleteRevisionsOperation(new DeleteRevisionsOperation.Parameters
                {
                    DocumentIds = new[] { id, id2, id3 }
                }));

                expectedSize = 348;
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

        private static void AssertStats(DocumentDatabase db, int bucket, int expectedSize, int expectedDocs = 1)
        {
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                Assert.Equal(expectedSize, stats.Size);
                Assert.Equal(expectedDocs, stats.NumberOfDocuments);
            }
        }
    }
}
