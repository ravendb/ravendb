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
    // Revision tombstones from pre-6.0 peers, whose wire key is the bare cv.Version without the document id.
    public class MixedModeRevisionPkTests : RavenTestBase
    {
        public MixedModeRevisionPkTests(ITestOutputHelper output) : base(output)
        {
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
    }
}
