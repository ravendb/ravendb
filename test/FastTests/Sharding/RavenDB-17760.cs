using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    public class RavenDB_17760 : RavenTestBase
    {
        public RavenDB_17760(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Sharding)]
        public async Task CanGetDocumentsByBucket()
        {
            using var store = Sharding.GetDocumentStore();
            var buckets = new int[3];

            for (int i = 0; i < 3; i++)
            {
                var suffix = $"suffix{i}";
                int bucket = ShardHelper.GetBucket(suffix);
                Assert.DoesNotContain(bucket, buckets);
                buckets[i] = bucket;

                using (var session = store.OpenAsyncSession())
                {
                    for (int j = 0; j < 100; j++)
                    {
                        await session.StoreAsync(new User(), $"users/{j}${suffix}");
                    }

                    await session.SaveChangesAsync();
                }
            }

            for (int i = 0; i < buckets.Length; i++)
            {
                var dbs = await Task.WhenAll(Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database));
                var notOnAnyShard = true;
                foreach (var shardedDb in dbs)
                {
                    using (shardedDb.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var docs = shardedDb.DocumentsStorage.GetDocumentsByBucketFrom(context, buckets[i], etag: 0).ToList();
                        if (docs.Count == 0)
                            continue;

                        notOnAnyShard = false;
                        Assert.Equal(100, docs.Count);
                        Assert.True(docs.Select(d => d.Id).All(s => s.EndsWith($"suffix{i}")));

                        var fromEtag = docs[70].Etag;
                        docs = shardedDb.DocumentsStorage.GetDocumentsByBucketFrom(context, buckets[i], etag: fromEtag).ToList();
                        Assert.Equal(30, docs.Count);
                        break;
                    }
                }

                Assert.False(notOnAnyShard);
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Sharding)]
        public async Task CanGetConflictsByBucket()
        {
            using var store = Sharding.GetDocumentStore();
            const string suffix = "suffix";
            int bucket = ShardHelper.GetBucket(suffix);

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${suffix}");
                }

                await session.SaveChangesAsync();
            }

            DocumentDatabase db = null;
            var dbs = await Task.WhenAll(Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database));
            foreach (var shard in dbs)
            {
                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var docs = shard.DocumentsStorage.GetDocumentsByBucketFrom(context, bucket, etag: 0).ToList();
                    if (docs.Count == 0)
                        continue;

                    db = shard;
                    break;
                }
            }

            Assert.NotNull(db);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var storage = db.DocumentsStorage;

                using (context.OpenWriteTransaction())
                {
                    // add some conflicts
                    for (int i = 0; i < 10; i++)
                    {
                        var id = $"users/{i}${suffix}";
                        var doc = context.ReadObject(new DynamicJsonValue(), id);
                        storage.ConflictsStorage.AddConflict(context, id, DateTime.UtcNow.Ticks, doc, $"incoming-cv-{i}", "users", DocumentFlags.None);
                    }

                    context.Transaction.Commit();
                }

                using (context.OpenReadTransaction())
                {
                    var conflicts = db.DocumentsStorage.ConflictsStorage.GetConflictsByBucketFrom(context, bucket, etag: 0).ToList();
                    var expected = 20; // 2 conflicts per doc
                    Assert.Equal(expected, conflicts.Count);


                    var fromEtag = conflicts[12].Etag;
                    conflicts = db.DocumentsStorage.ConflictsStorage.GetConflictsByBucketFrom(context, bucket, etag: fromEtag).ToList();
                    expected = 8; 
                    Assert.Equal(expected, conflicts.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Sharding)]
        public async Task CanGetTombstonesByBucket()
        {
            using var store = Sharding.GetDocumentStore();

            const string suffix = "suffix";
            int bucket = ShardHelper.GetBucket(suffix);

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${suffix}");
                }

                await session.SaveChangesAsync();
            }

            DocumentDatabase db = null;
            var dbs = await Task.WhenAll(Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database));
            foreach (var shard in dbs)
            {
                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var docs = shard.DocumentsStorage.GetDocumentsByBucketFrom(context, bucket, etag: 0).ToList();
                    if (docs.Count == 0)
                        continue;

                    db = shard;
                    break;
                }
            }

            Assert.NotNull(db);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (context.OpenWriteTransaction())
                {
                    // generate some tombstones
                    for (int i = 10; i < 30; i++)
                    {
                        var id = $"users/{i}${suffix}";
                        db.DocumentsStorage.Delete(context, id, expectedChangeVector: null);
                    }

                    context.Transaction.Commit();
                }

                using (context.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesByBucketFrom(context, bucket, etag: 0).ToList();
                    var expected = 20; 
                    Assert.Equal(expected, tombstones.Count);

                    for (int i = 0; i < tombstones.Count; i++)
                    {
                        var expectedId = $"users/{i + 10}${suffix}";

                        var item = tombstones[i] as DocumentReplicationItem;
                        Assert.NotNull(item);

                        var id = item.Id.ToString();

                        Assert.Equal(expectedId, id);
                    }


                    var fromEtag = tombstones[12].Etag;
                    tombstones = db.DocumentsStorage.GetTombstonesByBucketFrom(context, bucket, etag: fromEtag).ToList();
                    expected = 8;
                    Assert.Equal(expected, tombstones.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Sharding)]
        public async Task CanGetRevisionsByBucket()
        {
            using var store = Sharding.GetDocumentStore();
            const string suffix = "suffix";
            int bucket = ShardHelper.GetBucket(suffix);

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${suffix}");
                }

                await session.SaveChangesAsync();
            }

            await SetupRevisions(Server.ServerStore, store.Database);

            using (var session = store.OpenAsyncSession())
            {
                // generate some revisions
                for (int i = 10; i < 30; i++)
                {
                    var id = $"users/{i}${suffix}";
                    var doc = await session.LoadAsync<User>(id);
                    doc.Name = $"Name/{i}";
                }

                await session.SaveChangesAsync();
            }

            DocumentDatabase db = null;
            var dbs = await Task.WhenAll(Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database));
            foreach (var shard in dbs)
            {
                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var docs = shard.DocumentsStorage.GetDocumentsByBucketFrom(context, bucket, etag: 0).ToList();
                    if (docs.Count == 0)
                        continue;

                    db = shard;
                    break;
                }
            }

            Assert.NotNull(db);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var revisions = db.DocumentsStorage.RevisionsStorage.GetRevisionsByBucketFrom(context, bucket, etag: 0).ToList();
                    var expected = 40;
                    Assert.Equal(expected, revisions.Count);

                    var fromEtag = revisions[12].Etag;
                    revisions = db.DocumentsStorage.RevisionsStorage.GetRevisionsByBucketFrom(context, bucket, etag: fromEtag).ToList();
                    expected = 28;
                    Assert.Equal(expected, revisions.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Sharding)]
        public async Task CanGetAttachmentsByBucket()
        {
            using var store = Sharding.GetDocumentStore();
            const string suffix = "suffix";
            int bucket = ShardHelper.GetBucket(suffix);

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${suffix}");
                }

                await session.SaveChangesAsync();
            }

            var toDispose = new List<IDisposable>();
            try
            {
                using (var session = store.OpenAsyncSession())
                {
                    // generate some attachments

                    var rnd = new Random();
                    var size = 128;

                    for (int i = 10; i < 30; i++)
                    {
                        var id = $"users/{i}${suffix}";

                        var b = new byte[size];
                        rnd.NextBytes(b);
                        var stream = new MemoryStream(b);
                        toDispose.Add(stream);
                        session.Advanced.Attachments.Store(id, $"attachment/{i}", stream);
                    }

                    await session.SaveChangesAsync();
                }
            }
            finally
            {
                foreach (var stream in toDispose)
                {
                    stream.Dispose();
                }
            }

            DocumentDatabase db = null;
            var dbs = await Task.WhenAll(Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database));
            foreach (var shard in dbs)
            {
                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var docs = shard.DocumentsStorage.GetDocumentsByBucketFrom(context, bucket, etag: 0).ToList();
                    if (docs.Count == 0)
                        continue;

                    db = shard;
                    break;
                }
            }

            Assert.NotNull(db);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var attachments = db.DocumentsStorage.AttachmentsStorage.GetAttachmentsByBucketFrom(context, bucket, etag: 0).ToList();
                    var expected = 20;
                    Assert.Equal(expected, attachments.Count);
                    
                    for (int i = 0; i < attachments.Count; i++)
                    {
                        var expectedName = $"attachment/{i + 10}";

                        var item = attachments[i] as AttachmentReplicationItem;
                        Assert.NotNull(item);

                        Assert.Equal(expectedName, item.Name);
                    }

                    var fromEtag = attachments[12].Etag;
                    attachments = db.DocumentsStorage.AttachmentsStorage.GetAttachmentsByBucketFrom(context, bucket, etag: fromEtag).ToList();
                    expected = 8;
                    Assert.Equal(expected, attachments.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Sharding)]
        public async Task CanGetCountersByBucket()
        {
            using var store = Sharding.GetDocumentStore();
            const string suffix = "suffix";
            int bucket = ShardHelper.GetBucket(suffix);

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${suffix}");
                }

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                // generate some counters

                for (int i = 10; i < 30; i++)
                {
                    var id = $"users/{i}${suffix}";
                    var cf = session.CountersFor(id);

                    for (int j = 1; j <= 5; j++)
                    {
                        cf.Increment($"counter/{j}"); 
                    }
                }

                await session.SaveChangesAsync();
            }

            DocumentDatabase db = null;
            var dbs = await Task.WhenAll(Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database));
            foreach (var shard in dbs)
            {
                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var docs = shard.DocumentsStorage.GetDocumentsByBucketFrom(context, bucket, etag: 0).ToList();
                    if (docs.Count == 0)
                        continue;

                    db = shard;
                    break;
                }
            }

            Assert.NotNull(db);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var counters = db.DocumentsStorage.CountersStorage.GetCountersByBucketFrom(context, bucket, etag: 0).ToList();
                    var expected = 20;
                    Assert.Equal(expected, counters.Count);

                    for (int i = 0; i < counters.Count; i++)
                    {
                        var expectedId = $"users/{i + 10}${suffix}";

                        var item = counters[i] as CounterReplicationItem;
                        Assert.NotNull(item);
                        Assert.Equal(expectedId, item.Id.ToString());

                        Assert.True(item.Values.TryGet(CountersStorage.CounterNames, out BlittableJsonReaderObject names));
                        Assert.Equal(5, names.Count);

                        for (int j = 1; j <= 5; j++)
                        {
                            Assert.True(names.TryGet($"counter/{j}", out object _));
                        }
                    }

                    var fromEtag = counters[10].Etag;
                    counters = db.DocumentsStorage.CountersStorage.GetCountersByBucketFrom(context, bucket, etag: fromEtag).ToList();
                    expected = 10;
                    Assert.Equal(expected, counters.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Sharding)]
        public async Task CanGetTimeSeriesByBucket()
        {
            using var store = Sharding.GetDocumentStore();
            const string suffix = "suffix";
            int bucket = ShardHelper.GetBucket(suffix);

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${suffix}");
                }

                await session.SaveChangesAsync();
            }

            var baseline = DateTime.UtcNow;

            using (var session = store.OpenAsyncSession())
            {
                // generate some timeseries
                
                for (int i = 10; i < 30; i++)
                {
                    var id = $"users/{i}${suffix}";
                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int j = 1; j <= 100; j++)
                    {
                        tsf.Append(baseline.AddSeconds(j), j);
                    }
                }

                await session.SaveChangesAsync();
            }

            DocumentDatabase db = null;
            var dbs = await Task.WhenAll(Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database));
            foreach (var shard in dbs)
            {
                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var docs = shard.DocumentsStorage.GetDocumentsByBucketFrom(context, bucket, etag: 0).ToList();
                    if (docs.Count == 0)
                        continue;

                    db = shard;
                    break;
                }
            }

            Assert.NotNull(db);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var segments = db.DocumentsStorage.TimeSeriesStorage.GetSegmentsByBucketFrom(context, bucket, etag: 0).ToList();
                    var expected = 20;
                    Assert.Equal(expected, segments.Count);

                    var expectedNumberOfEntries = 100;
                    foreach (var item in segments)
                    {
                        Assert.Equal(expectedNumberOfEntries, item.Segment.NumberOfEntries);
                    }

                    var fromEtag = segments[10].Etag;
                    segments = db.DocumentsStorage.TimeSeriesStorage.GetSegmentsByBucketFrom(context, bucket, etag: fromEtag).ToList();
                    expected = 10;
                    Assert.Equal(expected, segments.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Sharding)]
        public async Task CanGetTimeSeriesDeletedRangesByBucket()
        {
            using var store = Sharding.GetDocumentStore();
            const string suffix = "suffix";
            int bucket = ShardHelper.GetBucket(suffix);

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${suffix}");
                }

                await session.SaveChangesAsync();
            }

            var baseline = DateTime.UtcNow.EnsureMilliseconds();

            using (var session = store.OpenAsyncSession())
            {
                // generate some timeseries

                for (int i = 10; i < 30; i++)
                {
                    var id = $"users/{i}${suffix}";
                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    for (int j = 1; j <= 100; j++)
                    {
                        tsf.Append(baseline.AddDays(j), j);
                    }
                }

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                // generate some timeseries-deleted-ranges items

                for (int i = 10; i < 30; i++)
                {
                    var id = $"users/{i}${suffix}";
                    var tsf = session.TimeSeriesFor(id, "HeartRate");

                    tsf.Delete(baseline.AddDays(10), baseline.AddDays(90));
                }

                await session.SaveChangesAsync();
            }

            DocumentDatabase db = null;
            var dbs = await Task.WhenAll(Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database));
            foreach (var shard in dbs)
            {
                using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var docs = shard.DocumentsStorage.GetDocumentsByBucketFrom(context, bucket, etag: 0).ToList();
                    if (docs.Count == 0)
                        continue;

                    db = shard;
                    break;
                }
            }

            Assert.NotNull(db);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    var deletedRanges = db.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesByBucketFrom(context, bucket, etag: 0).ToList();
                    var expected = 20;
                    Assert.Equal(expected, deletedRanges.Count);

                    for (int i = 0; i < deletedRanges.Count; i++)
                    {
                        Assert.Equal(baseline.AddDays(10), deletedRanges[i].From);
                        Assert.Equal(baseline.AddDays(90), deletedRanges[i].To);
                    }

                    var fromEtag = deletedRanges[10].Etag;
                    deletedRanges = db.DocumentsStorage.TimeSeriesStorage.GetDeletedRangesByBucketFrom(context, bucket, etag: fromEtag).ToList();
                    expected = 10;
                    Assert.Equal(expected, deletedRanges.Count);
                }
            }
        }

        private static async Task<long> SetupRevisions(Raven.Server.ServerWide.ServerStore serverStore, string database)
        {
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 5
                },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        PurgeOnDelete = true,
                        MinimumRevisionsToKeep = 123
                    }
                }
            };

            var index = await RevisionsHelper.SetupRevisions(serverStore, database, configuration);

            var documentDatabases = serverStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(database);
            foreach (var task in documentDatabases)
            {
                var db = await task;
                await db.RachisLogIndexNotifications.WaitForIndexNotification(index, serverStore.Engine.OperationTimeout);
            }

            return index;
        }
    }
}

