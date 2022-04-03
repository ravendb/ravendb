using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Operations;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Client.Operations
{
    public class GetDocumentConflictsTests : RavenTestBase
    {
        const string _suffix = "suffix";

        public GetDocumentConflictsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Revisions)]
        public async Task CanGetReplicationDocumentConflictsByEtag()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${_suffix}");
                }

                await session.SaveChangesAsync();
            }

            DocumentDatabase db = await GetDocumentDatabaseInstanceFor(store, store.Database);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                AddConflicts(context, db.DocumentsStorage);

                using (context.OpenReadTransaction())
                {
                    var conflicts = db.DocumentsStorage.ConflictsStorage.GetConflictsAfter(context, etag: 0).ToList();
                    var expected = 20; // 2 conflicts per doc
                    Assert.Equal(expected, conflicts.Count);

                    var fromEtag = conflicts[12].Etag;

                    var totalResults = db.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context);
                    expected = 10;
                    Assert.Equal(expected, totalResults);

                    var documentsConflict = await store.Maintenance.SendAsync(new GetReplicationConflictsOperation(etag: 0));
                    Assert.Equal(expected, documentsConflict.Results.Length);

                    documentsConflict = await store.Maintenance.SendAsync(new GetReplicationConflictsOperation(etag: fromEtag));
                    Assert.Equal(4, documentsConflict.Results.Length);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Sharding | RavenTestCategory.Revisions)]
        public async Task CanGetReplicationDocumentConflictsByEtagForSharding()
        {
            using var store = Sharding.GetDocumentStore();

            int bucket = ShardHelper.GetBucket(_suffix);

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${_suffix}");
                }

                await session.SaveChangesAsync();
            }

            var db = await GetShardedDocumentDatabase(store.Database, bucket);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                AddConflicts(context, db.DocumentsStorage);

                using (context.OpenReadTransaction())
                {
                    var conflicts = db.DocumentsStorage.ConflictsStorage.GetConflictsByBucketFrom(context, bucket, etag: 0).ToList();
                    Assert.Equal(20, conflicts.Count); // 2 conflicts per doc

                    var fromEtag = conflicts[12].Etag;

                    var documentsConflict = await store.Maintenance.SendAsync(new GetReplicationConflictsOperation());
                    Assert.Equal(10, documentsConflict.Results.Length);

                    documentsConflict = await store.Maintenance.SendAsync(new GetReplicationConflictsOperation(etag: fromEtag));
                    Assert.Equal(4, documentsConflict.Results.Length);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Revisions)]
        public async Task CanGetReplicationDocumentConflictsByDocId()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${_suffix}");
                }

                await session.SaveChangesAsync();
            }

            DocumentDatabase db = await GetDocumentDatabaseInstanceFor(store, store.Database);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                AddConflicts(context, db.DocumentsStorage);

                using (context.OpenReadTransaction())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var id = $"users/{i}${_suffix}";
                        var conflicts = db.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, id).ToList();
                        Assert.Equal(2, conflicts.Count); // 2 conflicts per doc

                        var documentConflicts = await store.Maintenance.SendAsync(new GetConflictsOperation(id));
                        Assert.Equal(2, documentConflicts.Results.Length);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Sharding | RavenTestCategory.Revisions)]
        public async Task CanGetReplicationDocumentConflictsByDocIdForSharding()
        {
            using var store = Sharding.GetDocumentStore();

            int bucket = ShardHelper.GetBucket(_suffix);

            using (var session = store.OpenAsyncSession())
            {
                for (int j = 0; j < 100; j++)
                {
                    await session.StoreAsync(new User(), $"users/{j}${_suffix}");
                }

                await session.SaveChangesAsync();
            }

            var db = await GetShardedDocumentDatabase(store.Database, bucket);

            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                AddConflicts(context, db.DocumentsStorage);
            }

            for (int i = 0; i < 10; i++)
            {
                var id = $"users/{i}${_suffix}";
                var documentConflicts = await store.Maintenance.SendAsync(new GetConflictsOperation(id));
                Assert.Equal(2, documentConflicts.Results.Length);
            }
        }

        private void AddConflicts(DocumentsOperationContext context, DocumentsStorage storage)
        {
            using (context.OpenWriteTransaction())
            {
                // add some conflicts
                for (int i = 0; i < 10; i++)
                {
                    var id = $"users/{i}${_suffix}";
                    var doc = context.ReadObject(new DynamicJsonValue(), id);
                    storage.ConflictsStorage.AddConflict(context, id, DateTime.UtcNow.Ticks, doc, $"incoming-cv-{i}", "users", DocumentFlags.None);
                }

                context.Transaction.Commit();
            }
        }

        private async Task<DocumentDatabase> GetShardedDocumentDatabase(string databaseName, int bucket)
        {
            DocumentDatabase db = null;
            var dbs = await Task.WhenAll(Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(databaseName));
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

            return db;
        }
    }
}
