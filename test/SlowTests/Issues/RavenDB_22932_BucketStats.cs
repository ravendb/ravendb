using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_22932_BucketStats : RavenTestBase
    {
        public RavenDB_22932_BucketStats(ITestOutputHelper output) : base(output)
        {
        }

        private const string Id = "companies/1";

        // Compaction copies the BucketStats tree verbatim while it re-inserts the rows, so the stats have to
        // survive it in two senses: they have to still describe what is there, and they have to stay usable
        // afterwards. The accounting is incremental, so the base compaction copied and every delta that comes
        // later have to be in the same units - today they are, because Size counts the raw size of the value
        // rather than the stored one. If RavenDB-19530 ever changes that, this is where it shows.
        [RavenFact(RavenTestCategory.Sharding)]
        public async Task CompactionLeavesTheBucketStatsAsTheyWere()
        {
            using (var store = Sharding.GetDocumentStore(new Options { RunInMemory = false }))
            {
                var bucket = await Sharding.GetBucketAsync(store, Id);
                var shardName = ShardHelper.ToShardName(store.Database, await Sharding.GetShardNumberForAsync(store, Id));

                await StoreAsync(store);

                var (sizeBefore, docsBefore, changeVectorBefore) = await GetBucketStatsAsync(shardName, bucket);

                await CompactAsync(store, shardName);

                var (sizeAfter, docsAfter, changeVectorAfter) = await GetBucketStatsAsync(shardName, bucket);

                Assert.Equal(docsBefore, docsAfter);
                Assert.Equal(sizeBefore, sizeAfter);
                Assert.Equal(changeVectorBefore, changeVectorAfter);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(Id);
                    await session.SaveChangesAsync();
                }

                var db = await GetDocumentDatabaseInstanceForAsync(shardName);

                // ExecuteCleanup deletes only up to the lowest etag its subscribers report and swallows its own
                // errors, so it is retried rather than trusted once
                var removed = await WaitForValueAsync(async () =>
                {
                    await db.TombstoneCleaner.ExecuteCleanup();

                    using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                    using (ctx.OpenReadTransaction())
                    {
                        return ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket) == null;
                    }
                }, true);

                Assert.True(removed, "the bucket still has a stats entry after its documents were deleted and their tombstones cleaned");
            }
        }

        private static async Task StoreAsync(IDocumentStore store)
        {
            var array = new string[10_000];
            for (int i = 0; i < array.Length; i++)
                array[i] = "6";

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Company { Array = array }, Id);
                await session.SaveChangesAsync();
            }
        }

        private static async Task CompactAsync(IDocumentStore store, string shardName)
        {
            var operation = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
            {
                DatabaseName = shardName,
                Documents = true
            }));

            await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
        }

        private async Task<(long Size, long NumberOfDocuments, string MergedChangeVector)> GetBucketStatsAsync(string shardName, int bucket)
        {
            var db = await Sharding.GetAnyShardDocumentDatabaseInstanceFor(shardName);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                var stats = ShardedDocumentsStorage.GetBucketStatisticsFor(ctx, bucket);
                Assert.NotNull(stats);

                // HandleReshardingChanges confirms a migration through this reader, so it has to survive too
                var merged = db.ShardedDocumentsStorage.GetMergedChangeVectorInBucket(ctx, bucket)?.AsString();
                Assert.False(string.IsNullOrEmpty(merged), "the bucket has no merged change vector, so comparing it across compaction would prove nothing");

                return (stats.Size, stats.NumberOfDocuments, merged);
            }
        }

        private class Company
        {
            public string[] Array { get; set; }
        }
    }
}
