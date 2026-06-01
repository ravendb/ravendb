using System;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Server;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using Xunit;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Revisions;

namespace InterversionTests.Revisions
{
    // Strict assertions driven by OldDataFixture.SeedStandardAsync snapshots.
    internal static class EntityAssertions
    {
        public static async Task AssertClientSurvivingRevisionsAsync(
            IDocumentStore store,
            OldDatabaseState state,
            int expectedSurvivingRevisions,
            string label)
        {
            foreach (var (docId, snapshot) in state.CapturedCVs)
            {
                using var session = store.OpenAsyncSession();
                var md = await session.Advanced.Revisions.GetMetadataForAsync(docId, pageSize: 100);

                Assert.True(md.Count == expectedSurvivingRevisions,
                    $"[{label}] doc '{docId}': expected exactly {expectedSurvivingRevisions} surviving revisions, got {md.Count}");

                // Metadata is returned newest-first.
                Assert.Equal(snapshot.RevisionCVs[^1], md[0].GetString("@change-vector"));
                if (expectedSurvivingRevisions >= 2)
                {
                    Assert.Equal(snapshot.RevisionCVs[^2], md[1].GetString("@change-vector"));
                }

                var flags = md[0].GetString("@flags") ?? "";
                Assert.Contains("DeleteRevision", flags);
            }
        }

        // Requires a current-bits server (Voron-level access).
        public static void AssertStrictTombstones(
            DocumentDatabase database,
            OldDatabaseState state,
            string label)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                foreach (var (docId, snapshot) in state.CapturedCVs)
                {
                    AssertEvictedRevisionTombstones(ctx, database, docId, snapshot, label);
                    AssertAttachmentTombstoneExists(ctx, database, docId, label);
                }
            }
        }

        public static async Task AssertStrictTombstonesAsync(
            RavenServer server,
            string dbName,
            OldDatabaseState state,
            string label)
        {
            var database = await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(dbName);
            AssertStrictTombstones(database, state, label);
        }

        public static async Task AssertStrictTombstonesAsync(
            IDocumentStore store,
            RavenServer ravenServer,
            OldDatabaseState state,
            string label)
        {
            var database = await ravenServer.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            AssertStrictTombstones(database, state, label);
        }

        private static void AssertEvictedRevisionTombstones(
            DocumentsOperationContext ctx, DocumentDatabase database, string docId, EntitySnapshot snapshot, string label)
        {
            var tombTable = ctx.Transaction.InnerTransaction.OpenTable(
                database.DocumentsStorage.TombstonesSchema, RevisionsTombstonesSlice);

            foreach (var evictedCv in snapshot.EvictedRevisionCVs)
            {
                if (string.IsNullOrEmpty(evictedCv))
                    continue;

                var cv = ctx.GetChangeVector(evictedCv);
                using (RevisionsStorage.BuildRevisionKeys(ctx, cv, docId, out var keys))
                {
                    bool hit = database.DocumentsStorage.RevisionsStorage.TryReadRevisionTombstone(tombTable, in keys.Tombstone, out _);
                    Assert.True(hit,
                        $"[{label}] doc '{docId}': E2 tombstone for evicted CV '{evictedCv}' not reachable.");
                }
            }
        }

        private static void AssertAttachmentTombstoneExists(
            DocumentsOperationContext ctx, DocumentDatabase database, string docId, string label)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(ctx, docId, out Slice lowerIdSlice, out _))
            {
                var attTombTable = ctx.Transaction.InnerTransaction.OpenTable(
                    database.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);

                bool found = HasAnyRevisionAttachmentTombstoneForDoc(ctx, attTombTable, lowerIdSlice);
                Assert.True(found,
                    $"[{label}] doc '{docId}': no E4 revision-attachment-tombstone row found.");
            }
        }

        private static bool HasAnyRevisionAttachmentTombstoneForDoc(
            DocumentsOperationContext ctx, Table table, Slice lowerIdSlice)
        {
            unsafe
            {
                int prefixLen = lowerIdSlice.Size + 3;
                Span<byte> buf = stackalloc byte[256];
                if (prefixLen > buf.Length)
                    buf = new byte[prefixLen];

                lowerIdSlice.AsReadOnlySpan().CopyTo(buf);
                buf[lowerIdSlice.Size] = SpecialChars.RecordSeparator;
                buf[lowerIdSlice.Size + 1] = (byte)'r';
                buf[lowerIdSlice.Size + 2] = SpecialChars.RecordSeparator;

                using (Slice.From(ctx.Allocator, buf.Slice(0, prefixLen), out Slice prefixSlice))
                {
                    foreach (var _ in table.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
                        return true;
                }
            }
            return false;
        }
    }
}
