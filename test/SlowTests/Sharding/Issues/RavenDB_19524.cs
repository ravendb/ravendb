using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Attachments;
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
                    DocumentIds = new List<string>() { id }
                }));

            
                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shard = await Sharding.GetShardNumberForAsync(store, id);
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

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ShouldMapAttachmentTombstonesToTheRightShard()
        {
            const string id = "users/1";
            const string attName = "foo";

            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), id);
                    await session.SaveChangesAsync();
                }

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                {
                    store.Operations.Send(new PutAttachmentOperation("users/1", attName, profileStream, "image/png"));
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                    var att = await session.Advanced.Attachments.GetAsync(user, attName);
                    Assert.NotNull(att);
                }

                store.Operations.Send(new DeleteAttachmentOperation("users/1", attName));

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                    var att = await session.Advanced.Attachments.GetAsync(user, attName);
                    Assert.Null(att);
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shard = await Sharding.GetShardNumberForAsync(store, id);
                var bucket = Sharding.GetBucket(record.Sharding, id);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard)) as ShardedDocumentDatabase;
                Assert.NotNull(db);

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.GetTombstonesFrom(ctx, 0).ToList();
                    Assert.Equal(1, tombstones.Count);
                    Assert.IsType<AttachmentTombstoneReplicationItem>(tombstones[0]);
                    
                    var tombstonesByBucket = db.ShardedDocumentsStorage.GetTombstonesByBucketFrom(ctx, bucket, etag: 0).ToList();
                    Assert.Equal(1, tombstonesByBucket.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ShouldMapCounterTombstonesToTheRightShard()
        {
            const string id = "users/1";
            const string counterName = "likes";

            using (var store = Sharding.GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor(id).Increment(counterName);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                    var counter = await session.CountersFor(id).GetAsync(counterName);
                    Assert.NotNull(counter);
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var shard = await Sharding.GetShardNumberForAsync(store, id);
                var bucket = Sharding.GetBucket(record.Sharding, id);

                var db = await GetDocumentDatabaseInstanceFor(store, ShardHelper.ToShardName(store.Database, shard)) as ShardedDocumentDatabase;
                Assert.NotNull(db);

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.CountersStorage.GetCounterTombstonesByBucketFrom(ctx, bucket, 0).ToList();
                    Assert.Equal(0, tombstones.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.CountersFor(id).Delete(counterName);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.NotNull(user);
                    var counter = await session.CountersFor(id).GetAsync(counterName);
                    Assert.Null(counter);
                }

                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var tombstones = db.DocumentsStorage.CountersStorage.GetCounterTombstonesByBucketFrom(ctx, bucket, 0).ToList();
                    Assert.Equal(1, tombstones.Count);
                }
            }
        }
    }
}
