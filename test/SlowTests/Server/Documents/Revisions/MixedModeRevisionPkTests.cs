using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Binary;
using Sparrow.Json;
using Tests.Infrastructure;
using Tests.Infrastructure.Utils;
using Voron;
using Voron.Data.Tables;
using Xunit;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Revisions;
using static Raven.Server.Documents.Schemas.Tombstones;
using static Tests.Infrastructure.Utils.RevisionTestHelpers;

namespace SlowTests.Server.Documents.Revisions
{
    // Direct-Voron seeding of legacy rows; exercises mixed-mode read/write paths end-to-end (compound CVs throughout).
    public class MixedModeRevisionPkTests : RavenTestBase
    {
        public MixedModeRevisionPkTests(ITestOutputHelper output) : base(output)
        {
        }

        // Sets up a born-clean-disabled DB with a single seeded legacy revision row for `docId`/`collection`,
        // carrying the compound CV `("A", DbA, orderEtag) | ("B", DbB, versionEtag)`. Caller owns store
        // disposal. Returns the seeded revision's full CV as a string so tests stay context-agnostic.
        private async Task<(DocumentStore store, DocumentDatabase database, string compoundCv)> SetupLegacyRevisionAsync(
            long orderEtag = 7,
            long versionEtag = 11,
            string docId = "users/1",
            string collection = "Users")
        {
            var store = GetDocumentStore(new Options { ModifyDatabaseRecord = StripHashedRevisionPkToken });
            await RevisionsHelper.SetupRevisionsAsync(store);
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Seed" }, docId);
                await session.SaveChangesAsync();
            }

            string compoundCvString;
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var compoundCv = BuildCompound(context, order: ("A", DbA, orderEtag), version: ("B", DbB, versionEtag));
                RevisionLegacyRowSeeder.SeedLegacyRevisionRow(
                    context, database, docId, collection, compoundCv,
                    etag: database.DocumentsStorage.GenerateNextEtag());
                compoundCvString = compoundCv.AsString();
                tx.Commit();
            }

            return (store, database, compoundCvString);
        }

        // A peer on a replication protocol below ReplicationWithRevisionTombstones (60_000, RavenDB 6.0)
        // negotiates RevisionTombstonesWithId = false and emits a revision-tombstone wire key that is the
        // bare cv.Version with NO [lowerDocId][RS] prefix. When the referenced parent revision is on disk
        // we recover the docId from RevisionsTable.LowerId (covered by DecrementsCountForActualDoc below).
        // When it isn't -- orphan tombstone -- there's no source of truth for the docId, and silently
        // accepting the tombstone would drift the revision-count tree. The receive must fail loudly with
        // an actionable upgrade message pointing the operator at RevisionTombstonesWithId (RavenDB 6.0+).
        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task DocIdLessRevisionTombstone_OrphanFromPre6Peer_ThrowsRequiringUpgrade()
        {
            using (var store = GetDocumentStore())
            {
                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenWriteTransaction())
                {
                    LazyStringValue docIdLessWireKey = context.GetLazyString("A:7-" + DbB); // bare cv.Version, no [docId][RS]

                    // No parent revision seeded -> recovery lookup misses -> orphan path rejected.
                    var ex = Assert.Throws<InvalidOperationException>(() =>
                    {
                        using (database.DocumentsStorage.RevisionsStorage.BuildRevisionTombstoneKeyFromExternal(
                            context, docIdLessWireKey, collection: "Users", out _))
                        {
                        }
                    });

                    Assert.Contains("Upgrade the source peer", ex.Message);
                    Assert.Contains("RevisionTombstonesWithId", ex.Message);
                }
            }
        }

        // Pre-6.0 docId-less revision tombstones reach WriteRevisionTombstoneFromExternal with
        // tombstoneKey.DocIdSlice == Slices.Empty (wire key is the bare cv.Version, no [docId][RS]).
        // The parent-revision lookup is keyed by hash of cv and still succeeds, so the row IS deleted --
        // but the subsequent IncrementCountOfRevisions(-1) builds its tree key from the EMPTY docId
        // prefix instead of the actual document's prefix. Pre-fix: the document's count tree entry
        // is untouched -> the count drifts +1 above the real surviving-revision count, corrupting
        // quota / config-driven eviction downstream.
        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task DocIdLessRevisionTombstone_FromPre6Peer_DecrementsCountForActualDoc()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                DocumentDatabase database = await Databases.GetDocumentDatabaseInstanceFor(store);

                const string docId = "users/1";
                const string collection = "Users";

                // Seed two revisions so the count tree has a real entry for docId.
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "v1" }, docId);
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    User u = await session.LoadAsync<User>(docId);
                    u.Name = "v2";
                    await session.SaveChangesAsync();
                }

                long countBefore;
                string revCvToTombstone;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    countBefore = database.DocumentsStorage.RevisionsStorage.GetRevisionsCount(readCtx, docId);
                    Assert.True(countBefore >= 2, $"Expected at least 2 seeded revisions, got {countBefore}.");

                    // Pick any existing revision -- we just need a CV whose hashed-form key matches a live row.
                    (Document[] revisions, _) = database.DocumentsStorage.RevisionsStorage.GetRevisions(readCtx, docId, start: 0, take: 1);
                    revCvToTombstone = revisions.Single().ChangeVector;
                }

                // Drive the receive path with a docId-less wire item (pre-6.0 peer: Id = bare cv.Version).
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext writeCtx))
                using (var tx = writeCtx.OpenWriteTransaction())
                {
                    var item = new RevisionTombstoneReplicationItem
                    {
                        Id = writeCtx.GetLazyString(revCvToTombstone),            // bare cv, no [docId][RS]
                        Collection = writeCtx.GetLazyString(collection),
                        Flags = DocumentFlags.DeleteRevision,
                        LastModifiedTicks = DateTime.UtcNow.Ticks,
                        ChangeVector = "TS:1-" + DbB,
                    };

                    database.DocumentsStorage.RevisionsStorage.WriteRevisionTombstoneFromReplication(
                        writeCtx, item, tombstoneChangeVector: item.ChangeVector);
                    tx.Commit();
                }

                // Pre-fix: countAfter == countBefore (decrement landed under an empty docId prefix).
                // Post-fix: countAfter == countBefore - 1 (decrement landed under the actual docId).
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    long countAfter = database.DocumentsStorage.RevisionsStorage.GetRevisionsCount(readCtx, docId);
                    Assert.Equal(countBefore - 1, countAfter);
                }
            }
        }

        // A Conflicted re-Put on a Legacy parent is the one surviving upgrade-on-touch path -- the
        // MarkRevisionAsConflicted rewrite migrates the row to the hashed PK in a single write. The critical
        // invariant pinned here is rule B: field 12 (FullChangeVector) must carry the INCOMING full
        // canonical CV. A naive implementation would read `revision.ChangeVector` from the row, which on a
        // Legacy source is field 0 = version-only, and would silently violate rule B.
        [RavenFact(RavenTestCategory.Revisions)]
        public async Task ConflictedRePutOfLegacyParent_UpgradesRowAndCarriesIncomingFullChangeVector()
        {
            const string docId = "users/1";

            // Seed a Legacy revision row with a compound CV so "version" != "full".
            var (store, database, fullCvString) = await SetupLegacyRevisionAsync(docId: docId);
            using (store)
            {
                // Re-Put the same revision WITH the Conflicted flag. Routes through MarkRevisionAsConflicted,
                // which writes a fresh row at the hashed PK with field 12 from the incoming CV.
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext putCtx))
                using (var tx = putCtx.OpenWriteTransaction())
                {
                    Raven.Server.Utils.ChangeVector cv = putCtx.GetChangeVector(fullCvString);
                    using (var doc = RevisionLegacyRowSeeder.BuildBlittableDocument(putCtx, name: "Legacy"))
                    {
                        database.DocumentsStorage.RevisionsStorage.Put(
                            putCtx,
                            id: docId,
                            document: doc,
                            flags: DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.Conflicted | DocumentFlags.FromReplication,
                            nonPersistentFlags: NonPersistentDocumentFlags.None,
                            changeVector: cv,
                            lastModifiedTicks: DateTime.UtcNow.Ticks);
                    }
                    tx.Commit();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    var collection = database.DocumentsStorage.ExtractCollectionName(readCtx, "Users");
                    Table table = database.DocumentsStorage.RevisionsStorage.EnsureRevisionTableCreated(
                        readCtx.Transaction.InnerTransaction, collection);

                    Raven.Server.Utils.ChangeVector compoundCv = readCtx.GetChangeVector(fullCvString);
                    using (RevisionsStorage.BuildRevisionKey(readCtx.Allocator, compoundCv, out RevisionKey key))
                    {
                        Assert.True(table.ReadByKey(key.PrefixedHash, out TableValueReader hashTvr),
                            "Parent revision must be reachable at the prefixed hash PK after the Conflicted re-Put.");
                        Assert.False(table.ReadByKey(key.Raw, out _),
                            "Legacy raw-PK parent row must be gone after the upgrade.");

                        // Rule B: field 12 carries the INCOMING full canonical CV (with the order prefix),
                        // not the row-side version-only that ReadChangeVectorFromTvr would have produced on
                        // the Legacy row. This pins MarkRevisionAsConflicted's incoming-CV plumbing.
                        Raven.Server.Utils.ChangeVector cvOnRow = RevisionsStorage.ReadChangeVectorFromTvr(readCtx, ref hashTvr);
                        Assert.Equal(fullCvString, cvOnRow.AsString());

                        DocumentFlags flagsOnRow = DocumentsStorage.TableValueToFlags((int)RevisionsTable.Flags, ref hashTvr);
                        Assert.True(flagsOnRow.Contain(DocumentFlags.Conflicted),
                            $"Row flags must include Conflicted after the marking re-Put; got {flagsOnRow}.");
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task LegacyRevisionRow_ReadFallbackByChangeVector()
        {
            var (store, database, compoundCvString) = await SetupLegacyRevisionAsync();
            using (store)
            {
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    var doc = database.DocumentsStorage.RevisionsStorage.GetRevision(readCtx, compoundCvString);
                    Assert.NotNull(doc);

                    // L-shape rows return version-only from field 0 -- order prefix is discarded (DESIGN.md §12).
                    var versionOnly = compoundCvString.Split('|')[1];
                    Assert.Equal(versionOnly, doc.ChangeVector);
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task LegacyRevisionTombstoneRow_ReadFallbackByChangeVector()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = StripHashedRevisionPkToken
            }))
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Seed" }, "users/1");
                    await session.SaveChangesAsync();
                }

                string deletedRevCvString;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var deletedRevCv = RevisionTestHelpers.BuildCompound(
                        context,
                        order: ("A", DbA, 21),
                        version: ("B", DbB, 22));
                    var tombstoneOwnCv = RevisionTestHelpers.BuildCompound(
                        context,
                        order: ("A", DbA, 23),
                        version: ("B", DbB, 24));

                    RevisionLegacyRowSeeder.SeedLegacyRevisionTombstoneRow(
                        context, database,
                        docId: "users/1",
                        collection: "Users",
                        deletedRevisionCv: deletedRevCv,
                        tombstoneOwnCv: tombstoneOwnCv.AsString(),
                        deletedRevisionEtag: 99);

                    deletedRevCvString = deletedRevCv.AsString();
                    tx.Commit();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    var deletedRevCv = readCtx.GetChangeVector(deletedRevCvString);
                    using (RevisionsStorage.BuildRevisionKeys(readCtx, deletedRevCv, "users/1", out RevisionKeys keys))
                    {
                        var table = readCtx.Transaction.InnerTransaction.OpenTable(
                            database.DocumentsStorage.TombstonesSchema, RevisionsTombstonesSlice);

                        Assert.False(table.ReadByKey(keys.Tombstone.HashComposite, out _));
                        Assert.True(table.ReadByKey(keys.Tombstone.RawComposite, out var tvr));
                        Assert.True(database.DocumentsStorage.RevisionsStorage.TryReadRevisionTombstone(table, in keys.Tombstone, out _));

                        unsafe
                        {
                            Assert.Equal((byte)Tombstone.TombstoneType.Revision,
                                *tvr.Read((int)TombstoneTable.Type, out _));
                        }
                    }
                }
            }
        }

        // RevisionsStorage.Put short-circuits same-CV writes, so WriteRevisionTableRecord is invoked directly.
        [RavenFact(RavenTestCategory.Revisions)]
        public async Task InsertDisplacesLegacyRow()
        {
            var (store, database, compoundCvString) = await SetupLegacyRevisionAsync();
            using (store)
            {
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext writeCtx))
                using (var tx = writeCtx.OpenWriteTransaction())
                {
                    var cv = writeCtx.GetChangeVector(compoundCvString);
                    var collectionName = database.DocumentsStorage.ExtractCollectionName(writeCtx, "Users");
                    var table = database.DocumentsStorage.RevisionsStorage.EnsureRevisionTableCreated(
                        writeCtx.Transaction.InnerTransaction, collectionName);

                    using (RevisionsStorage.BuildRevisionKeys(writeCtx, cv, "users/1", out RevisionKeys keys))
                    using (var doc = RevisionLegacyRowSeeder.BuildBlittableDocument(writeCtx, name: "Updated"))
                    {
                        var ticks = DateTime.UtcNow.Ticks;
                        unsafe
                        {
                            database.DocumentsStorage.RevisionsStorage.WriteRevisionTableRecord(writeCtx, table, in keys, new RevisionsTableRow
                            {
                                EtagSwapBytes = Bits.SwapBytes(database.DocumentsStorage.GenerateNextEtag()),
                                DocumentPtr = doc.BasePointer,
                                DocumentSize = doc.Size,
                                Flags = DocumentFlags.Revision,
                                DeletedEtagOrMarker = 0,
                                LastModifiedTicks = ticks,
                                TransactionMarker = writeCtx.GetTransactionMarker(),
                                ResolvedField = 0,
                                FullChangeVector = compoundCvString
                            }, isInsert: false);
                        }
                    }

                    tx.Commit();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    var collectionName = database.DocumentsStorage.ExtractCollectionName(readCtx, "Users");
                    var table = database.DocumentsStorage.RevisionsStorage.EnsureRevisionTableCreated(
                        readCtx.Transaction.InnerTransaction, collectionName);

                    var cv = readCtx.GetChangeVector(compoundCvString);
                    using (RevisionsStorage.BuildRevisionKey(readCtx.Allocator, cv, out var key))
                    {
                        Assert.True(table.ReadByKey(key.PrefixedHash, out var hashTvr));
                        Assert.False(table.ReadByKey(key.Raw, out _));

                        var fullCv = RevisionsStorage.ReadChangeVectorFromTvr(readCtx, ref hashTvr);
                        Assert.Equal(compoundCvString, fullCv.AsString());
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Voron)]
        public async Task CompactionPreservesMixedPkForms()
        {
            // CompactDatabaseOperation requires DirectoryStorageEnvironmentOptions -- RunInMemory must be false.
            var path = NewDataPath();
            using (var store = GetDocumentStore(new Options
            {
                Path = path,
                RunInMemory = false,
                ModifyDatabaseRecord = StripHashedRevisionPkToken
            }))
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Seed" }, "users/1");
                    await session.SaveChangesAsync();
                }

                string legacyRevCvString, legacyTombCvString, legacyAttCvString;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (var tx = ctx.OpenWriteTransaction())
                {
                    var revCv = RevisionTestHelpers.BuildCompound(ctx, order: ("A", DbA, 1), version: ("B", DbB, 2));
                    RevisionLegacyRowSeeder.SeedLegacyRevisionRow(ctx, database, "users/1", "Users", revCv, etag: database.DocumentsStorage.GenerateNextEtag());
                    legacyRevCvString = revCv.AsString();

                    var deletedCv = RevisionTestHelpers.BuildCompound(ctx, order: ("A", DbA, 3), version: ("B", DbB, 4));
                    var tombOwnCv = RevisionTestHelpers.BuildCompound(ctx, order: ("A", DbA, 5), version: ("B", DbB, 6));
                    RevisionLegacyRowSeeder.SeedLegacyRevisionTombstoneRow(ctx, database, "users/1", "Users", deletedCv, tombOwnCv.AsString(), deletedRevisionEtag: 33);
                    legacyTombCvString = deletedCv.AsString();

                    var parentRevCv = RevisionTestHelpers.BuildCompound(ctx, order: ("A", DbA, 7), version: ("B", DbB, 8));
                    var attOwnCv = RevisionTestHelpers.BuildCompound(ctx, order: ("A", DbA, 9), version: ("B", DbB, 10));
                    SeedLegacyRevisionAttachmentRow(
                        ctx, database, "users/1", parentRevCv,
                        attachmentName: "att-a", base64Hash: Hash44A, contentType: "text/plain",
                        attachmentOwnCv: attOwnCv.AsString());
                    legacyAttCvString = parentRevCv.AsString();

                    SeedLegacyRevisionAttachmentTombstoneRow(
                        ctx, database, "users/1", parentRevCv,
                        attachmentName: "att-b", base64Hash: Hash44B, contentType: "text/plain",
                        tombstoneOwnCv: attOwnCv.AsString(),
                        attachmentEtag: 77);

                    tx.Commit();
                }

                var op = await store.Maintenance.Server.SendAsync(new CompactDatabaseOperation(new CompactSettings
                {
                    DatabaseName = store.Database,
                    Documents = true
                }));
                await op.WaitForCompletionAsync(TimeSpan.FromMinutes(2));

                database = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    var rev = database.DocumentsStorage.RevisionsStorage.GetRevision(readCtx, legacyRevCvString);
                    Assert.NotNull(rev);

                    var deletedCv = readCtx.GetChangeVector(legacyTombCvString);
                    using (RevisionsStorage.BuildRevisionKeys(readCtx, deletedCv, "users/1", out RevisionKeys keys))
                    {
                        var tombTable = readCtx.Transaction.InnerTransaction.OpenTable(
                            database.DocumentsStorage.TombstonesSchema, RevisionsTombstonesSlice);
                        Assert.True(database.DocumentsStorage.RevisionsStorage.TryReadRevisionTombstone(tombTable, in keys.Tombstone, out _));
                    }

                    var parentRevCv = readCtx.GetChangeVector(legacyAttCvString);
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(readCtx, "users/1", out Slice lowerIdSlice, out _))
                    using (RevisionsStorage.BuildRevisionKey(readCtx.Allocator, parentRevCv, out var parentRevKey))
                    using (AttachmentsStorage.BuildRevisionAttachmentPrefix(readCtx, in parentRevKey, lowerIdSlice, out var prefix))
                    {
                        var attTable = readCtx.Transaction.InnerTransaction.OpenTable(
                            database.DocumentsStorage.AttachmentsStorage.AttachmentsSchema, AttachmentsMetadataSlice);
                        var attachments = database.DocumentsStorage.AttachmentsStorage
                            .GetRevisionAttachmentsByPrefix(readCtx, attTable, prefix)
                            .ToList();
                        Assert.Contains(attachments, a => a.Name == "att-a");
                    }

                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(readCtx, "users/1", out Slice lowerIdSlice, out _))
                    using (RevisionsStorage.BuildRevisionKey(readCtx.Allocator, parentRevCv, out var parentRevKey))
                    {
                        var attTombTable = readCtx.Transaction.InnerTransaction.OpenTable(
                            database.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);

                        unsafe
                        {
                            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(readCtx, "att-b", out Slice lowerNameSlice, out _))
                            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(readCtx, "text/plain", out Slice lowerContentTypeSlice, out _))
                            using (Slice.From(readCtx.Allocator, Hash44B, out Slice hashSlice))
                            using (AttachmentsStorage.BuildRevisionAttachmentKey(
                                       readCtx, in parentRevKey,
                                       lowerIdSlice.Content.Ptr, lowerIdSlice.Size,
                                       lowerNameSlice.Content.Ptr, lowerNameSlice.Size,
                                       hashSlice,
                                       lowerContentTypeSlice.Content.Ptr, lowerContentTypeSlice.Size,
                                       out var fullKey))
                            {
                                Assert.True(attTombTable.ReadByKey(fullKey.RawComposite, out _));
                            }
                        }
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task MixedEtagIteration()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = StripHashedRevisionPkToken
            }))
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Seed" }, "users/1");
                    await session.SaveChangesAsync();
                }

                var legacyCvs = new List<string>();
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    for (int i = 0; i < 3; i++)
                    {
                        var cv = RevisionTestHelpers.BuildCompound(
                            context,
                            order: ("A", DbA, 100 + i),
                            version: ("B", DbB, 200 + i));
                        RevisionLegacyRowSeeder.SeedLegacyRevisionRow(context, database, "users/1", "Users", cv, etag: database.DocumentsStorage.GenerateNextEtag());
                        legacyCvs.Add(cv.AsString());
                    }
                    tx.Commit();
                }

                for (int i = 0; i < 3; i++)
                {
                    using var session = store.OpenAsyncSession();
                    var user = await session.LoadAsync<User>("users/1");
                    user.Name = "v" + i;
                    await session.SaveChangesAsync();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    var all = database.DocumentsStorage.RevisionsStorage
                        .GetRevisionsFrom(readCtx, etag: 0, take: long.MaxValue)
                        .ToList();

                    // 1 initial revision from the Session put + 3 legacy seeds + 3 hash puts = 7
                    Assert.Equal(7, all.Count);

                    for (int i = 1; i < all.Count; i++)
                        Assert.True(all[i].Etag > all[i - 1].Etag, $"Etag not increasing at index {i}");

                    // Legacy rows surface as recovered canonical CVs (compound
                    // `<dbOrder>|<legacyVersion>`), so match by version-segment suffix.
                    var allCvs = all.Select(d => d.ChangeVector).ToList();
                    foreach (var legacyCv in legacyCvs)
                    {
                        var versionOnly = legacyCv.Split('|')[1];
                        Assert.Contains(allCvs, c => c != null && c.EndsWith(versionOnly));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Attachments)]
        public async Task AttachmentPrefixScan_WalksBothPkForms()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = StripHashedRevisionPkToken
            }))
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Seed" }, "users/1");
                    await session.SaveChangesAsync();
                }

                string parentRevCvString;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var parentRevCv = RevisionTestHelpers.BuildCompound(
                        context,
                        order: ("A", DbA, 7),
                        version: ("B", DbB, 11));
                    parentRevCvString = parentRevCv.AsString();

                    SeedLegacyRevisionAttachmentRow(context, database, "users/1", parentRevCv,
                        attachmentName: "a", base64Hash: Hash44A, contentType: "text/plain",
                        attachmentOwnCv: RevisionTestHelpers.BuildSingle(context, "A", DbA, 100).AsString());
                    SeedLegacyRevisionAttachmentRow(context, database, "users/1", parentRevCv,
                        attachmentName: "b", base64Hash: Hash44B, contentType: "text/plain",
                        attachmentOwnCv: RevisionTestHelpers.BuildSingle(context, "A", DbA, 101).AsString());

                    SeedHashRevisionAttachmentRow(context, database, "users/1", parentRevCv,
                        attachmentName: "c", base64Hash: Hash44C, contentType: "text/plain",
                        attachmentOwnCv: RevisionTestHelpers.BuildSingle(context, "A", DbA, 102).AsString());

                    tx.Commit();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    var parentRevCv = readCtx.GetChangeVector(parentRevCvString);
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(readCtx, "users/1", out Slice lowerIdSlice, out _))
                    using (RevisionsStorage.BuildRevisionKey(readCtx.Allocator, parentRevCv, out var parentRevKey))
                    using (AttachmentsStorage.BuildRevisionAttachmentPrefix(readCtx, in parentRevKey, lowerIdSlice, out var prefix))
                    {
                        var attTable = readCtx.Transaction.InnerTransaction.OpenTable(
                            database.DocumentsStorage.AttachmentsStorage.AttachmentsSchema, AttachmentsMetadataSlice);

                        var attachments = database.DocumentsStorage.AttachmentsStorage
                            .GetRevisionAttachmentsByPrefix(readCtx, attTable, prefix)
                            .ToList();

                        var names = attachments.Select(a => a.Name.ToString()).OrderBy(n => n).ToList();
                        Assert.Equal(new[] { "a", "b", "c" }, names);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Attachments)]
        public async Task AttachmentTombstone_DualFormReadByKey()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = StripHashedRevisionPkToken
            }))
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Seed" }, "users/1");
                    await session.SaveChangesAsync();
                }

                string parentRevCvString;
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    var parentRevCv = RevisionTestHelpers.BuildCompound(
                        context, order: ("A", DbA, 7), version: ("B", DbB, 11));
                    parentRevCvString = parentRevCv.AsString();

                    SeedLegacyRevisionAttachmentTombstoneRow(
                        context, database, "users/1", parentRevCv,
                        attachmentName: "x", base64Hash: Hash44A, contentType: "text/plain",
                        tombstoneOwnCv: RevisionTestHelpers.BuildSingle(context, "A", DbA, 555).AsString(),
                        attachmentEtag: 555);
                    tx.Commit();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    var parentRevCv = readCtx.GetChangeVector(parentRevCvString);
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(readCtx, "users/1", out Slice lowerIdSlice, out _))
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(readCtx, "x", out Slice lowerNameSlice, out _))
                    using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(readCtx, "text/plain", out Slice lowerCtSlice, out _))
                    using (Slice.From(readCtx.Allocator, Hash44A, out Slice hashSlice))
                    using (RevisionsStorage.BuildRevisionKey(readCtx.Allocator, parentRevCv, out var parentRevKey))
                    {
                        unsafe
                        {
                            using (AttachmentsStorage.BuildRevisionAttachmentKey(
                                       readCtx, in parentRevKey,
                                       lowerIdSlice.Content.Ptr, lowerIdSlice.Size,
                                       lowerNameSlice.Content.Ptr, lowerNameSlice.Size,
                                       hashSlice,
                                       lowerCtSlice.Content.Ptr, lowerCtSlice.Size,
                                       out var pair))
                            {
                                var table = readCtx.Transaction.InnerTransaction.OpenTable(
                                    database.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);

                                Assert.False(table.ReadByKey(pair.HashComposite, out _));
                                Assert.True(table.ReadByKey(pair.RawComposite, out _));
                                Assert.True(database.DocumentsStorage.AttachmentsStorage.TryReadRevisionAttachmentTombstoneByKey(
                                    table, in pair, out _));
                            }
                        }
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task ConflictOverLegacyRevision_ConvergesToHashPkForm()
        {
            var (store, database, compoundCvString) = await SetupLegacyRevisionAsync();
            using (store)
            {
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext putCtx))
                using (var tx = putCtx.OpenWriteTransaction())
                {
                    var cv = putCtx.GetChangeVector(compoundCvString);
                    using var doc = RevisionLegacyRowSeeder.BuildBlittableDocument(putCtx, name: "Conflicted");

                    database.DocumentsStorage.RevisionsStorage.Put(
                        putCtx,
                        id: "users/1",
                        document: doc,
                        flags: DocumentFlags.HasRevisions | DocumentFlags.Revision | DocumentFlags.Conflicted | DocumentFlags.FromReplication,
                        nonPersistentFlags: NonPersistentDocumentFlags.None,
                        changeVector: cv,
                        lastModifiedTicks: DateTime.UtcNow.Ticks);

                    tx.Commit();
                }

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext readCtx))
                using (readCtx.OpenReadTransaction())
                {
                    var collectionName = database.DocumentsStorage.ExtractCollectionName(readCtx, "Users");
                    var table = database.DocumentsStorage.RevisionsStorage.EnsureRevisionTableCreated(
                        readCtx.Transaction.InnerTransaction, collectionName);

                    var cv = readCtx.GetChangeVector(compoundCvString);
                    using (RevisionsStorage.BuildRevisionKey(readCtx.Allocator, cv, out var key))
                    {
                        Assert.True(table.ReadByKey(key.PrefixedHash, out var tvr));
                        Assert.False(table.ReadByKey(key.Raw, out _));

                        var fullCv = RevisionsStorage.ReadChangeVectorFromTvr(readCtx, ref tvr);
                        Assert.Equal(compoundCvString, fullCv.AsString());

                        unsafe
                        {
                            int size;
                            var flagsPtr = tvr.Read((int)RevisionsTable.Flags, out size);
                            var flagsValue = *(int*)flagsPtr;
                            Assert.True(((DocumentFlags)flagsValue).Contain(DocumentFlags.Conflicted),
                                $"Expected Conflicted flag set after conflict marking; got {(DocumentFlags)flagsValue}.");
                        }
                    }
                }
            }
        }

        private static unsafe void SeedLegacyRevisionAttachmentRow(
            DocumentsOperationContext context,
            DocumentDatabase database,
            string docId,
            Raven.Server.Utils.ChangeVector parentRevisionCv,
            string attachmentName,
            string base64Hash,
            string contentType,
            string attachmentOwnCv)
        {
            SeedRevisionAttachmentRowInternal(context, database, docId, parentRevisionCv, attachmentName,
                base64Hash, contentType, attachmentOwnCv, useRawPkForm: true);
        }

        // Bypasses the dual-form write guard so hash and raw rows can coexist for testing.
        private static unsafe void SeedHashRevisionAttachmentRow(
            DocumentsOperationContext context,
            DocumentDatabase database,
            string docId,
            Raven.Server.Utils.ChangeVector parentRevisionCv,
            string attachmentName,
            string base64Hash,
            string contentType,
            string attachmentOwnCv)
        {
            SeedRevisionAttachmentRowInternal(context, database, docId, parentRevisionCv, attachmentName,
                base64Hash, contentType, attachmentOwnCv, useRawPkForm: false);
        }

        private static unsafe void SeedRevisionAttachmentRowInternal(
            DocumentsOperationContext context,
            DocumentDatabase database,
            string docId,
            Raven.Server.Utils.ChangeVector parentRevisionCv,
            string attachmentName,
            string base64Hash,
            string contentType,
            string attachmentOwnCv,
            bool useRawPkForm)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(
                database.DocumentsStorage.AttachmentsStorage.AttachmentsSchema, AttachmentsMetadataSlice);
            var newEtag = database.DocumentsStorage.GenerateNextEtag();

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice lowerId, out _))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachmentName, out Slice lowerName, out Slice nameStorage))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, contentType, out Slice lowerCt, out Slice ctStorage))
            using (Slice.From(context.Allocator, base64Hash, out Slice hashSlice))
            using (RevisionsStorage.BuildRevisionKey(context.Allocator, parentRevisionCv, out var parentRevKey))
            using (AttachmentsStorage.BuildRevisionAttachmentKey(
                       context, in parentRevKey,
                       lowerId.Content.Ptr, lowerId.Size,
                       lowerName.Content.Ptr, lowerName.Size,
                       hashSlice,
                       lowerCt.Content.Ptr, lowerCt.Size,
                       out var pair))
            using (Slice.From(context.Allocator, attachmentOwnCv, out Slice cvSlice))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                Slice pkSlice = useRawPkForm ? pair.RawComposite : pair.HashComposite;

                tvb.Add(pkSlice.Content.Ptr, pkSlice.Size);                    // 0 composite PK
                tvb.Add(Bits.SwapBytes(newEtag));                              // 1 Etag
                tvb.Add(nameStorage.Content.Ptr, nameStorage.Size);            // 2 Name
                tvb.Add(ctStorage.Content.Ptr, ctStorage.Size);                // 3 ContentType
                tvb.Add(hashSlice.Content.Ptr, hashSlice.Size);                // 4 Hash
                tvb.Add(context.GetTransactionMarker());                       // 5 TransactionMarker
                tvb.Add(cvSlice.Content.Ptr, cvSlice.Size);                    // 6 ChangeVector
                // v7.2 schema baseline -- pre-22358 RA rows already had Size/Flags/RemoteAt/Identifier;
                // AttachmentsFlagAndHashSlice dynamic index requires Flags so the row must include it.
                tvb.Add(0L);                                                   // 7 Size (test row has no stream)
                tvb.Add(Bits.SwapBytes((int)Raven.Client.Documents.Attachments.RemoteAttachmentFlags.None)); // 8 Flags
                tvb.Add(-1L);                                                  // 9 RemoteAt (sentinel for "no remote")
                tvb.Add(Slices.Empty.Content.Ptr, Slices.Empty.Size);          // 10 Identifier

                table.Insert(tvb);
            }
        }

        private static unsafe void SeedLegacyRevisionAttachmentTombstoneRow(
            DocumentsOperationContext context,
            DocumentDatabase database,
            string docId,
            Raven.Server.Utils.ChangeVector parentRevisionCv,
            string attachmentName,
            string base64Hash,
            string contentType,
            string tombstoneOwnCv,
            long attachmentEtag)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(
                database.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);
            var newEtag = database.DocumentsStorage.GenerateNextEtag();

            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, docId, out Slice lowerId, out _))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, attachmentName, out Slice lowerName, out _))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, contentType, out Slice lowerCt, out _))
            using (Slice.From(context.Allocator, base64Hash, out Slice hashSlice))
            using (RevisionsStorage.BuildRevisionKey(context.Allocator, parentRevisionCv, out var parentRevKey))
            using (AttachmentsStorage.BuildRevisionAttachmentKey(
                       context, in parentRevKey,
                       lowerId.Content.Ptr, lowerId.Size,
                       lowerName.Content.Ptr, lowerName.Size,
                       hashSlice,
                       lowerCt.Content.Ptr, lowerCt.Size,
                       out var pair))
            using (Slice.From(context.Allocator, tombstoneOwnCv, out Slice cv))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(pair.RawComposite.Content.Ptr, pair.RawComposite.Size);  // 0 raw composite PK
                tvb.Add(Bits.SwapBytes(newEtag));                              // 1 Etag
                tvb.Add(Bits.SwapBytes(attachmentEtag));                       // 2 DeletedEtag
                tvb.Add(context.GetTransactionMarker());                       // 3 TransactionMarker
                tvb.Add((byte)Tombstone.TombstoneType.Attachment);             // 4 Type
                tvb.Add(null, 0);                                              // 5 Collection (mirror CreateRevisionAttachmentTombstone)
                tvb.Add((int)DocumentFlags.None);                              // 6 Flags
                tvb.Add(cv.Content.Ptr, cv.Size);                              // 7 ChangeVector
                tvb.Add(DateTime.UtcNow.Ticks);                                // 8 LastModified

                table.Insert(tvb);
            }
        }

    }
}
