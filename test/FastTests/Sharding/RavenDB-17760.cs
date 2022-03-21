using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
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


            /*var dbs = await Task.WhenAll(Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(store.Database));

            // get all documents from all shards, and map them to bucket number
            var buckets = new Dictionary<long, List<Document>>();
            foreach (var db2 in dbs)
            {
                var storage = db.DocumentsStorage;
                using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var items = storage.GetDocumentsFrom(context, etag: 0, start: 0, take: int.MaxValue);
                    using (db.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext txContext))
                    {
                        foreach (var item in items)
                        {
                            var bucket = ShardedContext.GetShardId(txContext, item.Id);
                            if (buckets.TryGetValue(bucket, out var list) == false)
                            {
                                buckets[bucket] = list = new List<Document>();
                            }
                            list.Add(item);
                        }
                    }
                }
            }

            // foreach bucket, get all the docs in this bucket (from all shards)
            // and assert that it matches the mapping we constructed earlier 

            foreach (var kvp in buckets)
            {
                var key = kvp.Key;
                var docsInBucket = new List<Document>();
                foreach (var db4 in dbs)
                {
                    var storage = db.DocumentsStorage;
                    using (storage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var items = storage.GetDocumentsByBucketFrom(context, bucket: 11, etag: 0);
                        docsInBucket.AddRange(items);
                    }
                }

                Assert.True(kvp.Value.SequenceEqual(docsInBucket.OrderBy(d => d.Etag)));
            }*/
        }

    }
}
