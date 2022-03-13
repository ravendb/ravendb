using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sharding
{
    public class RavenDB_17760 : ShardedTestBase
    {
        public RavenDB_17760(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanGetDocumentsByBucket()
        {
            using var store = GetShardedDocumentStore();
            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database).First();
            var buckets = new int[3];

            for (int i = 0; i < 3; i++)
            {
                var suffix = $"suffix{i}";
                using (db.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext txContext))
                {
                    var bucket = ShardedContext.GetShardId(txContext, suffix);
                    Assert.DoesNotContain(bucket, buckets);
                    buckets[i] = bucket;
                }

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

        [Fact]
        public async Task CanGetConflictsByBucket()
        {
            using var store = GetShardedDocumentStore();
            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database).First();

            const string suffix = "suffix";
            int bucket;
            using (db.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext txContext))
            {
                bucket = ShardedContext.GetShardId(txContext, suffix);
            }

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${suffix}");
                }

                await session.SaveChangesAsync();
            }

            db = null;
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
                    var conflicts = ConflictsStorage.GetConflictsByBucketFrom(context, bucket, etag: 0).ToList();
                    var expected = 20; // 2 conflicts per doc
                    Assert.Equal(expected, conflicts.Count);


                    var fromEtag = conflicts[12].Etag;
                    conflicts = ConflictsStorage.GetConflictsByBucketFrom(context, bucket, etag: fromEtag).ToList();
                    expected = 8; 
                    Assert.Equal(expected, conflicts.Count);
                }
            }
        }
    }
}

