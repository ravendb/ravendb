using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Orders;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit.Abstractions;
using static Xunit.Assert;

namespace SlowTests.Sharding.Cluster
{
    public class ReshardingTests : ClusterTestBase
    {
        public ReshardingTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveOneBucketManually()
        {
            DoNotReuseServer();
            using (var store = Sharding.GetDocumentStore())
            {
                Server.ServerStore.Sharding.ManualMigration = true;

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                var id = "foo/bar";
                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = "Original shard"
                    };
                    await session.StoreAsync(user, id);
                    await session.SaveChangesAsync();
                }

                var bucket = ShardHelper.GetBucket(id);
                var location = ShardHelper.GetShardNumber(record.Sharding.BucketRanges, bucket);
                var newLocation = (location + 1) % record.Sharding.Shards.Length;
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    NotNull(user);
                }

                var result = await Server.ServerStore.Sharding.StartBucketMigration(store.Database, bucket, location, newLocation);

                var exists = WaitForDocument<User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, newLocation));
                True(exists);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>(id);
                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    await Server.ServerStore.Sharding.SourceMigrationCompleted(store.Database, bucket, result.Index, changeVector);
                }

                result = await Server.ServerStore.Sharding.DestinationMigrationConfirm(store.Database, bucket, result.Index);
                await Server.ServerStore.Cluster.WaitForIndexNotification(result.Index);

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.Name = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Equal("Original shard", user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveOneBucketOfSampleData()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation( /*| DatabaseItemType.Attachments | DatabaseItemType.CounterGroups | DatabaseItemType.RevisionDocuments*/));

                var id = "orders/830-A";
                var oldLocation = await Sharding.GetShardNumber(store, id);
               
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var order = await session.LoadAsync<Order>(id);
                    NotNull(order);
                }

                await Sharding.Resharding.MoveShardForId(store, id);

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(id);
                    order.Employee = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var order = await session.LoadAsync<Order>(id);
                    Null(order);
                }

                var newLocation = await Sharding.GetShardNumber(store, id);
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var order = await session.LoadAsync<Order>(id);
                    NotNull(order);
                }

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "assert for everything");
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveOneBucket()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));

                var id = "foo/bar";
                var bucket = ShardHelper.GetBucket(id);
                var location = ShardHelper.GetShardNumber(record.Sharding.BucketRanges, bucket);
                var newLocation = (location + 1) % record.Sharding.Shards.Length;
                using (var session = store.OpenAsyncSession())
                {
                    var user = new User
                    {
                        Name = "Original shard"
                    };
                    await session.StoreAsync(user, id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    NotNull(user);
                }

                await Sharding.Resharding.MoveShardForId(store, id);

                var exists = WaitForDocument<User>(store, id, predicate: null, database: ShardHelper.ToShardName(store.Database, newLocation));
                True(exists);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>(id);
                    NotNull(user);
                }

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.Name = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, location)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Null(user);
                }
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>(id);
                    Equal("New shard", user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveBucketWhileWriting()
        {
            using var store = Sharding.GetDocumentStore();

            var writes = Task.Run(() =>
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                    }, "users/1-A");
                    session.SaveChanges();
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                        }, $"num-{i}$users/1-A");
                        session.SaveChanges();
                    }
                }
            });

            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");

            await writes;

            await AssertWaitForValueAsync(async () =>
            {
                using (var session = store.OpenAsyncSession())
                {
                    var q = await session.Query<User>().ToListAsync();
                    return q.Count;
                }
            }, 101);

            var expectedShard = await Sharding.GetShardNumber(store, "users/1-A");
            for (int shard = 0; shard < 3; shard++)
            {
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, shard)))
                {
                    var q = await session.Query<User>().ToListAsync();
                    Equal(expectedShard == shard ? 101 : 0, q.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveBucketWhileWriting2()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using var store = Sharding.GetDocumentStore(new Options
            {
                Server = cluster.Leader
            });

            var writes = Task.Run(() =>
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Count = 10
                    }, "users/1-A");
                    session.SaveChanges();
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Count = 666
                        },"users/");
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Count = 10
                        }, $"num-{i}$users/1-A");
                        session.SaveChanges();
                    }
                }
            });

            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");
            await Sharding.Resharding.MoveShardForId(store, "users/1-A");

            await writes;

            await AssertWaitForValueAsync(async () =>
            {
                using (var session = store.OpenAsyncSession())
                {
                    var q = await session.Query<User>().Where(u => u.Count == 10, exact: true).ToListAsync();
                    return q.Count;
                }
            }, 101);

            var expectedShard = await Sharding.GetShardNumber(store, "users/1-A");
            for (int shard = 0; shard < 3; shard++)
            {
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, shard)))
                {
                    var q = await session.Query<User>().Where(u => u.Count == 10, exact: true).ToListAsync();
                    Equal(expectedShard == shard ? 101 : 0, q.Count);
                }
            }
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
                    Assert.Equal(10, stats.NumberOfItems);
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
                    Assert.Equal(10, stats.NumberOfItems);
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
                    Assert.Equal(1411, stats.Size);
                    Assert.Equal(5, stats.NumberOfItems);
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

                // TODO
                /* 
                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, 1));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {

                    var stats = ShardedDocumentsStorage.GetBucketStatistics(ctx, start: 0).ToList();
                    Assert.Equal(3, stats.Count);

                    foreach (var bucketStats in stats)
                    {
                        Assert.True(buckets.TryGetValue(bucketStats.Bucket, out (int NumOfDocs, int Size) val));

                        Assert.Equal(val.Size, bucketStats.Size);
                        Assert.Equal(val.NumOfDocs, bucketStats.NumberOfItems);
                        Assert.True(bucketStats.LastAccessed > before);
                        Assert.True(bucketStats.LastAccessed < after);
                    }

                    Assert.True(stats[0].Bucket < stats[1].Bucket);
                    Assert.True(stats[1].Bucket < stats[2].Bucket);
                }
                */

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
                    Assert.Equal(expected.NumOfDocs, stats.NumberOfItems);
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
                    Assert.Equal(expected.NumOfDocs, stats.NumberOfItems);
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
                    Assert.Equal(expected.NumOfDocs, stats.NumberOfItems);
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
                    var stats = ShardedDocumentsStorage.GetBucketStatistics(ctx, start: 0).ToList();
                    Assert.Equal(bucketsInShard0.Count, stats.Count);

                    foreach (var bucketStats in stats)
                    {
                        Assert.Equal(numOfDocsPerBucket, bucketStats.NumberOfItems);
                        Assert.True(bucketStats.Size > 0);
                    }
                }
            }
        }
    }
}

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task BucketDeletionShouldCreateArtificialTombstones()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                const string id = "orders/1";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order(), id);
                    await session.SaveChangesAsync();
                }

                var oldLocation = await Sharding.GetShardNumber(store, id);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var order = await session.LoadAsync<Order>(id);
                    NotNull(order);
                }

                await Sharding.Resharding.MoveShardForId(store, id);

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(id);
                    order.Employee = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var order = await session.LoadAsync<Order>(id);
                    Null(order);
                }

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Equal(1, tombs.Count);

                    var tomb = (DocumentReplicationItem)tombs[0];
                    Equal(id.ToLower(), tomb.Id.ToString(CultureInfo.InvariantCulture));
                    Equal(ReplicationBatchItem.ReplicationItemType.DocumentTombstone, tomb.Type);
                    True(tomb.Flags.Contain(DocumentFlags.Artificial));
                }

                var newLocation = await Sharding.GetShardNumber(store, id);
                NotEqual(oldLocation, newLocation);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var order = await session.LoadAsync<Order>(id);
                    Equal("New shard", order.Employee);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task BucketDeletionShouldCreateArtificialTombstones2()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                await InsertData(store);

                var suffix = "usa";
                var id1 = $"users/1${suffix}";
                var id2 = $"users/2${suffix}";
                var id3 = $"users/3${suffix}";
                var id4 = $"users/4${suffix}";

                var oldLocation = await Sharding.GetShardNumber(store, id1);

                await Sharding.Resharding.MoveShardForId(store, id1);

                // the document will be written to the new location
                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id1);
                    user.AddressId = "New shard";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var user = await session.LoadAsync<User>(id1);
                    Null(user);

                    user = await session.LoadAsync<User>(id2);
                    Null(user);

                    user = await session.LoadAsync<User>(id3);
                    Null(user);

                    user = await session.LoadAsync<User>(id4);
                    Null(user);
                }

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Equal(20, tombs.Count);

                    foreach (var item in tombs)
                    {
                        DocumentFlags flags = item switch
                        {
                            DocumentReplicationItem documentReplicationItem => documentReplicationItem.Flags,
                            AttachmentTombstoneReplicationItem attachmentTombstone => attachmentTombstone.Flags,
                            RevisionTombstoneReplicationItem revisionTombstone => revisionTombstone.Flags,
                            _ => DocumentFlags.None
                        };

                        True(flags.Contain(DocumentFlags.Artificial));
                    }
                }

                var newLocation = await Sharding.GetShardNumber(store, id1);
                NotEqual(oldLocation, newLocation);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>(id1);
                    NotNull(user);

                    user = await session.LoadAsync<User>(id2);
                    NotNull(user);

                    user = await session.LoadAsync<User>(id3);
                    NotNull(user);

                    user = await session.LoadAsync<User>(id4);
                    NotNull(user);
                }

                await CheckData(store, database: ShardHelper.ToShardName(store.Database, newLocation));
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task BucketDeletionShouldMarkExistingTombstonesAsArtificial()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                const string suffix = "eu";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), $"users/1${suffix}");
                    await session.StoreAsync(new User(), $"users/2${suffix}");
                    await session.StoreAsync(new User(), $"users/3${suffix}");
                    await session.StoreAsync(new User(), $"users/4${suffix}");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete($"users/2${suffix}");
                    session.Delete($"users/3${suffix}");
                    session.Delete($"users/4${suffix}");

                    await session.SaveChangesAsync();
                }

                var id = $"users/1${suffix}";
                var oldLocation = await Sharding.GetShardNumber(store, id);

                var oldLocationShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (oldLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = oldLocationShard.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Equal(3, tombs.Count);

                    foreach (var tomb in tombs)
                    {
                        var replicationItem = tomb as DocumentReplicationItem;
                        NotNull(replicationItem);
                        False(replicationItem.Flags.Contain(DocumentFlags.Artificial));
                    }
                }

                await Sharding.Resharding.MoveShardForId(store, id);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var user = await session.LoadAsync<User>($"users/1${suffix}");
                    Null(user);
                }

                using (oldLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = oldLocationShard.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Equal(4, tombs.Count);

                    foreach (var tomb in tombs)
                    {
                        var replicationItem = tomb as DocumentReplicationItem;
                        NotNull(replicationItem);
                        True(replicationItem.Flags.Contain(DocumentFlags.Artificial));
                    }
                }

                var newLocation = await Sharding.GetShardNumber(store, id);
                NotEqual(oldLocation, newLocation);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var user = await session.LoadAsync<User>($"users/1${suffix}");
                    NotNull(user);
                }

                var newLocationShard = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, newLocation));
                using (newLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = newLocationShard.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Equal(3, tombs.Count);

                    foreach (var tomb in tombs)
                    {
                        var replicationItem = tomb as DocumentReplicationItem;
                        NotNull(replicationItem);
                        False(replicationItem.Flags.Contain(DocumentFlags.Artificial));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CanMoveBucketWhenLastItemIsNotDocument()
        {
            using var store = Sharding.GetDocumentStore();

            const string id = "users/1-A";
            using (var session = store.OpenSession())
            {
                session.Store(new User(), id);
                session.CountersFor(id).Increment("Likes", 100);
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                session.CountersFor(id).Increment("Likes", 100);
                session.SaveChanges();
            }

            var oldLocation = await Sharding.GetShardNumber(store, id);

            await Sharding.Resharding.MoveShardForId(store, id);

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
            {
                var user = await session.LoadAsync<User>(id);
                Null(user);
            }

            var newLocation = await Sharding.GetShardNumber(store, id);
            NotEqual(oldLocation, newLocation);

            using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
            {
                var user = await session.LoadAsync<User>(id);
                NotNull(user);

                var counter = await session.CountersFor(user).GetAsync("Likes");
                Equal(200, counter);
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ShouldNotReplicateTombstonesCreatedByBucketDeletion()
        {
            using (var store = Sharding.GetDocumentStore())
            using (var replica = GetDocumentStore())
            {
                await InsertData(store);

                var suffix = "$usa";
                var id = $"users/1{suffix}";

                await SetupReplication(store, replica);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.AddressId = "New";
                    await session.SaveChangesAsync();
                }

                True(WaitForDocument<User>(replica, id, u => u.AddressId == "New"));

                await CheckData(replica);

                var oldLocation = await Sharding.GetShardNumber(store, id);

                await Sharding.Resharding.MoveShardForId(store, id);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Equal(22, tombs.Count);
                }

                var newLocation = await Sharding.GetShardNumber(store, id);

                await CheckData(store, ShardHelper.ToShardName(store.Database, newLocation));

                await CheckData(replica);
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task IndexesShouldTakeIntoAccountArtificialTombstones()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                const string suffix = "usa";

                await new UsersByName().ExecuteAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(10));

                    for (int i = 1; i <= 10; i++)
                    {
                        var docId = $"users/{i}${suffix}";
                        await session.StoreAsync(new User
                        {
                            Name = $"Name-{i}",
                        }, docId);
                    }

                    await session.SaveChangesAsync();
                }

                var id = $"users/1${suffix}";
                var oldLocation = await Sharding.GetShardNumber(store, id);

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var q = await session.Query<User, UsersByName>().ToListAsync();
                    Equal(10, q.Count);
                }

                await Sharding.Resharding.MoveShardForId(store, id);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Equal(10, tombs.Count);
                }

                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    var q = await session.Query<User, UsersByName>().ToListAsync();
                    Equal(0, q.Count);
                }

                var newLocation = await Sharding.GetShardNumber(store, id);
                using (var session = store.OpenAsyncSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    var q = await session.Query<User, UsersByName>().ToListAsync();
                    Equal(10, q.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task EtlShouldNotSendTombstonesCreatedByBucketDeletion()
        {
            using (var store = Sharding.GetDocumentStore())
            using (var replica = GetDocumentStore())
            {
                await InsertData(store);

                AddEtl(store, replica);

                var suffix = "usa";
                var id = $"users/1${suffix}";

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    user.AddressId = "New";
                    await session.SaveChangesAsync();
                }

                True(WaitForDocument<User>(replica, id, u => u.AddressId == "New"));

                await CheckData(replica, expectedRevisionsCount: 0);

                var oldLocation = await Sharding.GetShardNumber(store, id);

                await Sharding.Resharding.MoveShardForId(store, id);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, oldLocation));
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombs = db.DocumentsStorage.GetTombstonesFrom(context, 0).ToList();
                    Equal(22, tombs.Count);
                }

                var newLocation = await Sharding.GetShardNumber(store, id);

                await CheckData(store, ShardHelper.ToShardName(store.Database, newLocation));

                await CheckData(replica, expectedRevisionsCount: 0);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport | RavenTestCategory.Sharding)]
        public async Task RestoreShardedDatabaseFromIncrementalBackupAfterBucketMigration()
        {
            const string suffix = "eu";
            var backupPath = NewDataPath(suffix: "BackupFolder");
            var cluster = await CreateRaftCluster(3, watcherCluster: true);

            var options = Sharding.GetOptionsForCluster(cluster.Leader, shards: 3, shardReplicationFactor: 1, orchestratorReplicationFactor: 3);
            using (var store = Sharding.GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        session.Store(new User(), $"users/{i}${suffix}");
                    }

                    session.SaveChanges();
                }

                var waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

                var config = Backup.CreateBackupConfiguration(backupPath);
                var backupTaskId = await Sharding.Backup.UpdateConfigurationAndRunBackupAsync(cluster.Nodes, store, config, isFullBackup: false);

                True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                // migrate bucket
                const string id = $"users/1${suffix}";
                var oldLocation = await Sharding.GetShardNumber(store, id);
                await Sharding.Resharding.MoveShardForId(store, id);

                // add more data
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), $"users/11${suffix}");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                {
                    for (int i = 1; i <= 11; i++)
                    {
                        var doc = session.Load<User>($"users/{i}${suffix}");
                        Null(doc);
                    }
                }

                var newLocation = await Sharding.GetShardNumber(store, id);
                using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, newLocation)))
                {
                    for (int i = 1; i <= 11; i++)
                    {
                        var doc = session.Load<User>($"users/{i}${suffix}");
                        NotNull(doc);
                    }
                }

                // run backup again
                waitHandles = await Sharding.Backup.WaitForBackupsToComplete(cluster.Nodes, store.Database);

                await Sharding.Backup.RunBackupAsync(store.Database, backupTaskId, isFullBackup: false, cluster.Nodes);
                True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                var dirs = Directory.GetDirectories(backupPath);
                Equal(cluster.Nodes.Count, dirs.Length);

                foreach (var dir in dirs)
                {
                    var indexOf = dir.LastIndexOf('$');
                    True(indexOf > -1);

                    var shardIndex = int.Parse(dir[indexOf + 1].ToString());
                    True(shardIndex is >= 0 and <= 2);

                    var files = Directory.GetFiles(dir);
                    if (shardIndex == oldLocation || shardIndex == newLocation)
                        Equal(2, files.Length);
                    else
                        Equal(1, files.Length);
                }

                var sharding = await Sharding.GetShardingConfigurationAsync(store);
                var settings = Sharding.Backup.GenerateShardRestoreSettings(dirs, sharding);

                // restore the database with a different name
                var restoredDatabaseName = $"restored_database-{Guid.NewGuid()}";
                using (Sharding.Backup.ReadOnly(backupPath))
                using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration
                {
                    DatabaseName = restoredDatabaseName,
                    ShardRestoreSettings = settings
                }, timeout: TimeSpan.FromSeconds(60)))
                {
                    var dbRec = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(restoredDatabaseName));
                    Equal(3, dbRec.Sharding.Shards.Length);

                    var server = cluster.Nodes.Single(n => n.ServerStore.NodeTag == sharding.Shards[oldLocation].Members[0]);
                    var oldLocationShard = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(ShardHelper.ToShardName(restoredDatabaseName, oldLocation));
                    using (oldLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var docs = oldLocationShard.DocumentsStorage.GetDocumentsFrom(ctx, 0).ToList();
                        Equal(0, docs.Count);

                        var tombs = oldLocationShard.DocumentsStorage.GetTombstonesFrom(ctx, 0).ToList();
                        Equal(10, tombs.Count);
                        All(tombs, t => ((DocumentReplicationItem)t).Flags.Contain(DocumentFlags.Artificial));
                    }

                    server = cluster.Nodes.Single(n => n.ServerStore.NodeTag == sharding.Shards[newLocation].Members[0]);
                    var newLocationShard = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(ShardHelper.ToShardName(restoredDatabaseName, newLocation));
                    using (newLocationShard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        var docs = newLocationShard.DocumentsStorage.GetDocumentsFrom(ctx, 0).ToList();
                        Equal(11, docs.Count);

                        var tombs = newLocationShard.DocumentsStorage.GetTombstonesFrom(ctx, 0).ToList();
                        Equal(0, tombs.Count);
                    }

                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Major,
                        "Preserve bucket ranges on backup and restore : https://issues.hibernatingrhinos.com/issue/RavenDB-19160/");
                    /*using (var session = store.OpenSession(restoredDatabaseName))
                    {
                        for (int i = 1; i <= 11; i++)
                        {
                            var doc = session.Load<User>($"users/{i}${suffix}");
                            NotNull(doc);
                        }
                    }*/
                }
            }
        }

        private static void AddEtl(IDocumentStore source, IDocumentStore destination)
        {
            var taskName = "etl-test";
            var csName = "cs-test";

            var configuration = new RavenEtlConfiguration
            {
                ConnectionStringName = csName, 
                Name = taskName, 
                Transforms =
                {
                    new Transformation
                    {
                        Name = "S1", 
                        Collections = { "Users" }
                    }
                }
            };

            var connectionString = new RavenConnectionString
            {
                Name = csName, 
                TopologyDiscoveryUrls = destination.Urls, 
                Database = destination.Database,
            };

            var putResult = source.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(connectionString));
            NotNull(putResult.RaftCommandIndex);
            source.Maintenance.Send(new AddEtlOperation<RavenConnectionString>(configuration));
        }

        private static async Task InsertData(IDocumentStore store)
        {
            var suffix = "usa";
            var id1 = $"users/1${suffix}";
            var id2 = $"users/2${suffix}";
            var id3 = $"users/3${suffix}";
            var id4 = $"users/4${suffix}";

            using (var session = store.OpenAsyncSession())
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false
                    }
                }));

                //Docs
                await session.StoreAsync(new User { Name = "Name1", LastName = "LastName1", Age = 5 }, id1);
                await session.StoreAsync(new User { Name = "Name2", LastName = "LastName2", Age = 78 }, id2);
                await session.StoreAsync(new User { Name = "Name3", LastName = "LastName3", Age = 4 }, id3);
                await session.StoreAsync(new User { Name = "Name4", LastName = "LastName4", Age = 15 }, id4);

                //Time series
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Aviv, DevelopmentHelper.Severity.Major, "fix timeseries migration");
                /*session.TimeSeriesFor(id1, "Heartrate")
                    .Append(DateTime.Now, 59d, "watches/fitbit");
                session.TimeSeriesFor(id2, "Heartrate")
                    .Append(DateTime.Now.AddHours(6), 59d, "watches/fitbit");*/

                //counters
                session.CountersFor(id3).Increment("Downloads", 100);

                //Attachments
                var names = new[]
                {
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt",
                    "profile.png"
                };

                await using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                await using (var backgroundStream = new MemoryStream(new byte[] { 10, 20, 30, 40, 50 }))
                await using (var fileStream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }))
                {
                    session.Advanced.Attachments.Store(id1, names[0], backgroundStream, "ImGgE/jPeG");
                    session.Advanced.Attachments.Store(id2, names[1], fileStream);
                    session.Advanced.Attachments.Store(id3, names[2], profileStream, "image/png");
                    await session.SaveChangesAsync();
                }
            }
            
            // revision
            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(id1);
                user.Age = 10;
                await session.SaveChangesAsync();
            }
        }

        private async Task CheckData(IDocumentStore store, string database = null, long expectedRevisionsCount = 10)
        {
            database ??= store.Database;
            var db = await GetDocumentDatabaseInstanceFor(store, database);
            var storage = db.DocumentsStorage;

            var docsCount = storage.GetNumberOfDocuments();
            using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                //tombstones
                var tombstonesCount = storage.GetNumberOfTombstones(context);
                Equal(0, tombstonesCount);

                //revisions
                var revisionsCount = storage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
                Equal(expectedRevisionsCount, revisionsCount);
            }

            //docs
            Equal(4, docsCount);

            var suffix = "usa";
            using (var session = store.OpenSession(database))
            {
                //todo fix timeseries migration
                /*var val = session.TimeSeriesFor("users/1$usa", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);

                val = session.TimeSeriesFor("users/2$usa", "Heartrate")
                    .Get(DateTime.MinValue, DateTime.MaxValue);

                Assert.Equal(1, val.Length);*/

                //Counters
                var counterValue = session.CountersFor($"users/3${suffix}").Get("Downloads");
                Equal(100, counterValue.Value);
            }

            //Attachments
            using (var session = store.OpenAsyncSession(database))
            {
                var attachmentNames = new[]
                {
                    "background-photo.jpg",
                    "fileNAME_#$1^%_בעברית.txt",
                    "profile.png"
                };

                for (var i = 0; i < attachmentNames.Length; i++)
                {
                    var id = $"users/{i + 1}${suffix}";
                    var user = await session.LoadAsync<User>(id);
                    var metadata = session.Advanced.GetMetadataFor(user);

                    var attachments = metadata.GetObjects(Constants.Documents.Metadata.Attachments);
                    Equal(1, attachments.Length);
                    
                    var attachment = attachments[0];
                    var name = attachment.GetString(nameof(AttachmentName.Name));
                    var hash = attachment.GetString(nameof(AttachmentName.Hash));
                    var size = attachment.GetLong(nameof(AttachmentName.Size));

                    Equal(attachmentNames[i], name);

                    string expectedHash = default;
                    long expectedSize = default;

                    switch (i)
                    {
                        case 0:
                            expectedHash = "igkD5aEdkdAsAB/VpYm1uFlfZIP9M2LSUsD6f6RVW9U=";
                            expectedSize = 5;
                            break;
                        case 1:
                            expectedHash = "Arg5SgIJzdjSTeY6LYtQHlyNiTPmvBLHbr/Cypggeco=";
                            expectedSize = 5;
                            break;
                        case 2:
                            expectedHash = "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=";
                            expectedSize = 3;
                            break;
                    }

                    Equal(expectedHash, hash);
                    Equal(expectedSize, size);

                    var attachmentResult = await session.Advanced.Attachments.GetAsync(id, name);
                    NotNull(attachmentResult);
                }
            }
        }

        private static Task SetupReplication(IDocumentStore fromStore, IDocumentStore toStore)
        {
            var databaseWatcher = new ExternalReplication(toStore.Database, $"ConnectionString-{toStore.Identifier}");
            return ReplicationTestBase.AddWatcherToReplicationTopology(fromStore, databaseWatcher, fromStore.Urls);
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = users =>
                    from user in users
                    select new
                    {
                        user.Name
                    };
            }
        }
    }
}
