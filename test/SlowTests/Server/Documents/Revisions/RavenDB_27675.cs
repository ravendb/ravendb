using System;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron;
using Xunit;
using static Tests.Infrastructure.Utils.RevisionTestHelpers;

namespace SlowTests.Server.Documents.Revisions
{
    // A DB upgraded from 6.2 can hold a legacy raw-PK revision row and a hashed row with the same (DbId, Etag)
    // identity (the hash is tag-blind, the raw probe needs the exact string). Deleting one of them by key
    // removed both forms, and the twin that the deletion loop had already materialized then NRE'd.
    public class RavenDB_27675 : RavenTestBase
    {
        public RavenDB_27675(ITestOutputHelper output) : base(output)
        {
        }

        private const string DocId = "users/1";
        private const string Collection = "Users";

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task DeleteAllRevisions_LegacyRowWithHashedTwin_DeletesEachRowOnce()
        {
            using (var store = GetDocumentStore(new Options { ModifyDatabaseRecord = StripHashedRevisionPkToken }))
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                await StoreSeedAsync(store);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    SeedLegacyRow(context, database, "A:11-" + DbB);
                    PutReplicatedRevision(context, database, "SINK:11-" + DbB); // same identity, different tag
                    tx.Commit();
                }

                AssertRevisionRows(database, expected: 3);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    database.DocumentsStorage.RevisionsStorage.ForceDeleteAllRevisionsFor(context, DocId);
                    tx.Commit();
                }

                AssertRevisionRows(database, expected: 0);
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task PruneOnUpdate_LegacyRowWithHashedTwin_DeletesEachRowOnce()
        {
            using (var store = GetDocumentStore(new Options { ModifyDatabaseRecord = StripHashedRevisionPkToken }))
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                await StoreSeedAsync(store);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    SeedLegacyRow(context, database, "A:11-" + DbB);
                    PutReplicatedRevision(context, database, "SINK:11-" + DbB);
                    tx.Commit();
                }

                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: c => c.Collections[Collection].MinimumRevisionsToKeep = 1);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(DocId);
                    user.Name = "Updated";
                    await session.SaveChangesAsync();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var (revisions, count) = database.DocumentsStorage.RevisionsStorage.GetRevisions(context, DocId, 0, 10);
                    Assert.Equal(1, count);
                    Assert.Equal("Updated", revisions[0].Data["Name"].ToString());
                }
            }
        }

        // Born-clean token present: the raw probe is skipped, so a legacy row is invisible to key lookups.
        [RavenFact(RavenTestCategory.Revisions)]
        public async Task DeleteAllRevisions_LegacyRowUnderHashOnlyGate_DeletesByRowId()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                Assert.True(database.SupportedFeatures.SupportedFeatureTypes.HashedRevisionPk);
                await StoreSeedAsync(store);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    SeedLegacyRow(context, database, "A:11-" + DbB);
                    tx.Commit();
                }

                AssertRevisionRows(database, expected: 2);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    database.DocumentsStorage.RevisionsStorage.ForceDeleteAllRevisionsFor(context, DocId);
                    tx.Commit();
                }

                AssertRevisionRows(database, expected: 0);
            }
        }

        // The enumerated revision lives in another collection's table (doc deleted and recreated as Employee).
        [RavenFact(RavenTestCategory.Revisions)]
        public async Task DeleteAllRevisions_RevisionOwnedByAnotherCollectionTable_DeletesByRowId()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store, modifyConfiguration: c => c.Collections[Collection].PurgeOnDelete = false);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                await StoreSeedAsync(store);

                using (var session = store.OpenAsyncSession())
                {
                    session.Delete(DocId);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Employee { FirstName = "Moved" }, DocId);
                    await session.SaveChangesAsync();
                }

                AssertRevisionRows(database, expected: 3); // user revision, delete revision, employee revision

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    database.DocumentsStorage.RevisionsStorage.ForceDeleteAllRevisionsFor(context, DocId);
                    tx.Commit();
                }

                AssertRevisionRows(database, expected: 0);
            }
        }

        private static async Task StoreSeedAsync(Raven.Client.Documents.IDocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Seed" }, DocId);
                await session.SaveChangesAsync();
            }
        }

        private static void SeedLegacyRow(DocumentsOperationContext context, DocumentDatabase database, string changeVector)
        {
            RevisionLegacyRowSeeder.SeedLegacyRevisionRow(context, database, DocId, Collection,
                context.GetChangeVector(changeVector), etag: database.DocumentsStorage.GenerateNextEtag());

            using (DocumentIdWorker.GetLoweredIdSliceFromId(context, DocId, out Slice lowerId))
            using (database.DocumentsStorage.RevisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefix))
            {
                RevisionsStorage.IncrementCountOfRevisions(context, prefix, 1);
            }
        }

        private static void PutReplicatedRevision(DocumentsOperationContext context, DocumentDatabase database, string changeVector)
        {
            using (var doc = RevisionLegacyRowSeeder.BuildBlittableDocument(context, name: "Twin"))
            {
                database.DocumentsStorage.RevisionsStorage.Put(context, DocId, doc,
                    DocumentFlags.Revision | DocumentFlags.HasRevisions, NonPersistentDocumentFlags.FromReplication,
                    context.GetChangeVector(changeVector), DateTime.UtcNow.Ticks);
            }
        }

        private static void AssertRevisionRows(DocumentDatabase database, int expected)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var (revisions, count) = database.DocumentsStorage.RevisionsStorage.GetRevisions(context, DocId, 0, 10);
                Assert.Equal(expected, revisions.Length);
                Assert.Equal(expected, count);
            }
        }
    }
}
