using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Revisions;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_19524 : RavenTestBase
    {
        public RavenDB_19524(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ShouldMapRevisionTombstonesToTheRightShard()
        {
            const string id = "users/1";

            using (var store = Sharding.GetDocumentStore())
            {
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRevisionsOperation(new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = false
                    }
                }));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), id);
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 5; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>(id);
                        user.Name = i.ToString();
                        await session.SaveChangesAsync();
                    }
                }

                await store.Maintenance.SendAsync(new DeleteRevisionsOperation(new DeleteRevisionsOperation.Parameters
                {
                    DocumentIds = new[] { id }
                }));

            
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shard = await Sharding.GetShardNumberFor(store, id);
                var bucket = Sharding.GetBucket(record.Sharding, id);
              
                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard)) as ShardedDocumentDatabase;
                Assert.NotNull(db);

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesFrom(ctx, 0).ToList();
                    Assert.Equal(6, tombstones.Count);
                    foreach (var tombstone in tombstones)
                    {
                        Assert.IsType<RevisionTombstoneReplicationItem>(tombstone);
                    }

                    var tombstonesByBucket = db.ShardedDocumentsStorage.GetTombstonesByBucketFrom(ctx, bucket, etag: 0).ToList();
                    Assert.Equal(6, tombstonesByBucket.Count);
                }
            }
        }
    }
}
