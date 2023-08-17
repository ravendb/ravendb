using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using static Raven.Server.Documents.Replication.ReplicationItems.ReplicationBatchItem;

namespace SlowTests.Sharding.Issues
{
    public class RavenDB_21103 : ReplicationTestBase
    {
        public RavenDB_21103(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ShouldThrowOnDeleteDocumentFromWrongShardAfterResharding()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var id = "users/shiran";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Shiran" }, id);
                    await session.SaveChangesAsync();
                }

                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);
                await Sharding.Resharding.MoveShardForId(store, id);

                Assert.Throws<ShardMismatchException>(() =>
                {
                    using (var session = store.OpenSession(ShardHelper.ToShardName(store.Database, oldLocation)))
                    {
                        session.Delete(id);
                        session.SaveChanges();
                    }
                });
            }
        }

        [RavenFact(RavenTestCategory.Sharding)]
        public async Task ShardedDocumentsMigratorShouldMoveBucketWithTombstones()
        {
            using (var store = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = r => r.Settings["Tombstones.CleanupIntervalInMin"] = int.MaxValue.ToString()
            }))
            {
                var id = "users/shiran";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Shiran" }, id);
                    await session.SaveChangesAsync();
                }

                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);
                await Sharding.Resharding.MoveShardForId(store, id);

                var db1 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourceStore(ShardHelper.ToShardName(store.Database, oldLocation));
                db1.ForTestingPurposesOnly().EnableWritesToTheWrongShard = true;

                using (var session = store.OpenAsyncSession(db1.Name))
                {
                    await session.StoreAsync(new User { Name = "Shiran" }, id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(db1.Name))
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                await db1.DocumentsMigrator.ExecuteMoveDocumentsAsync();

                var dbName2 = await Sharding.GetShardDatabaseNameForDocAsync(store, id);
                Assert.True(WaitForValue(() =>
                {
                    using (var session = store.OpenSession(dbName2))
                    {
                        var user = session.Load<User>(id);
                        return user == null;
                    }
                }, true, 30_000, 333));
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Revisions)]
        public async Task ShardedDocumentsMigratorShouldMoveBucketWithRevisionTombstones()
        {
            using (var store = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = r => r.Settings["Tombstones.CleanupIntervalInMin"] = int.MaxValue.ToString()
            }))
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                var id = "users/shiran";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Shiran" }, id);
                    await session.SaveChangesAsync();
                }

                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);
                await Sharding.Resharding.MoveShardForId(store, id);

                var db1 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourceStore(ShardHelper.ToShardName(store.Database, oldLocation));

                await db1.TombstoneCleaner.ExecuteCleanup();
                db1.ForTestingPurposesOnly().EnableWritesToTheWrongShard = true;

                using (var session = store.OpenAsyncSession(db1.Name))
                {
                    await session.StoreAsync(new User { Name = "Shiran" }, id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession(db1.Name))
                {
                    session.Delete(id);
                    await session.SaveChangesAsync();
                }

                using (db1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = db1.DocumentsStorage.GetTombstonesFrom(context, 0).OrderBy(x => x.Etag).ToList();
                    Assert.Equal(2, tombstones.Count);

                    Assert.Equal(ReplicationItemType.DocumentTombstone, tombstones[0].Type);
                    Assert.Equal(ReplicationItemType.RevisionTombstone, tombstones[1].Type);
                }

                await db1.DocumentsMigrator.ExecuteMoveDocumentsAsync();

                var dbName2 = await Sharding.GetShardDatabaseNameForDocAsync(store, id);
                Assert.True(WaitForValue(() =>
                {
                    using (var session = store.OpenSession(dbName2))
                    {
                        var user = session.Load<User>(id);
                        return user == null;
                    }
                }, true, 30_000, 333));

                var db2 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourceStore(dbName2);

                using (db2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = db2.DocumentsStorage.GetTombstonesFrom(context, 0).OrderBy(x => x.Etag).ToList();
                    Assert.Equal(3, tombstones.Count);

                    Assert.Equal(ReplicationItemType.DocumentTombstone, tombstones[0].Type);
                    Assert.Equal(ReplicationItemType.RevisionTombstone, tombstones[1].Type);
                    Assert.Equal(ReplicationItemType.RevisionTombstone, tombstones[2].Type);
                }
            }
        }

        [RavenFact(RavenTestCategory.Sharding | RavenTestCategory.Attachments)]
        public async Task ShardedDocumentsMigratorShouldMoveBucketWithAttachmentTombstones()
        {
            using (var store = Sharding.GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = r => r.Settings["Tombstones.CleanupIntervalInMin"] = int.MaxValue.ToString()
            }))
            {
                var id = "users/shiran";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Shiran" }, id);
                    await session.SaveChangesAsync();
                }

                var oldLocation = await Sharding.GetShardNumberForAsync(store, id);
                await Sharding.Resharding.MoveShardForId(store, id);

                var db1 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourceStore(ShardHelper.ToShardName(store.Database, oldLocation));

                await db1.TombstoneCleaner.ExecuteCleanup();
                db1.ForTestingPurposesOnly().EnableWritesToTheWrongShard = true;

                using (var profileStream = new MemoryStream(new byte[] { 1, 2, 3 }))
                using (var session = store.OpenSession(db1.Name))
                {
                    session.Store(new User { Name = "Shiran2" }, id);
                    session.Advanced.Attachments.Store(id, "profile.png", profileStream, "image/png");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(db1.Name))
                {
                    session.Delete(id);
                    session.SaveChanges();
                }

                using (db1.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = db1.DocumentsStorage.GetTombstonesFrom(context, 0).OrderBy(x => x.Etag).ToList();
                    Assert.Equal(2, tombstones.Count);

                    Assert.Equal(ReplicationItemType.DocumentTombstone, tombstones[0].Type);
                    Assert.Equal(ReplicationItemType.AttachmentTombstone, tombstones[1].Type);
                }

                await db1.DocumentsMigrator.ExecuteMoveDocumentsAsync();

                var dbName2 = await Sharding.GetShardDatabaseNameForDocAsync(store, id);
                Assert.True(WaitForValue(() =>
                {
                    using (var session = store.OpenSession(dbName2))
                    {
                        var user = session.Load<User>(id);
                        return user == null;
                    }
                }, true, 30_000, 333));

                var db2 = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourceStore(dbName2);

                using (db2.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var tombstones = db2.DocumentsStorage.GetTombstonesFrom(context, 0).OrderBy(x => x.Etag).ToList();
                    Assert.Equal(2, tombstones.Count);

                    Assert.Equal(ReplicationItemType.DocumentTombstone, tombstones[0].Type);
                    Assert.Equal(ReplicationItemType.AttachmentTombstone, tombstones[1].Type);
                }
            }
        }
    }
}
