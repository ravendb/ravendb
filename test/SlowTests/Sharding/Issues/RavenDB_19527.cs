using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_19527 : RavenTestBase
    {
        public RavenDB_19527(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task TombstonesBucketAndEtagIndexShouldMapToSameBucket()
        {
            const string id = "foo/bar";
            var bucket = Sharding.GetBucket(id);

            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order(), id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shard = ShardHelper.GetShardNumberFor(record.Sharding, bucket);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard)) as ShardedDocumentDatabase;
                Assert.NotNull(db);

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var numberOfTombstones = db.DocumentsStorage.GetNumberOfTombstones(ctx);
                    Assert.Equal(2, numberOfTombstones);

                    var tombs = db.ShardedDocumentsStorage.GetTombstonesByBucketFrom(ctx, bucket, 0).ToList();
                    Assert.Equal(2, tombs.Count);
                }
            }
        }
    }
}
