// AttachmentsStorage RevisionAttachmentKey slice. Builds and resolves the
// composite PK of revision-attachment rows (and their tombstones, which share
// the same shape):
//
//   [lowerDocId][RS]'r'[RS][revCv-or-hash][RS][lowerName][RS][attHash44][RS][lowerContentType]
//
// The `[revCv-or-hash]` segment identifies the *parent revision*: its
// cv.Version bytes (Legacy row) or its 24-byte prefixed hash (Hashed row). `[attHash44]`
// is the attachment's own 44-byte base64 content hash. The same struct is
// reused for full keys, prefixes, and partial-key variants -- only segments
// past the revCv slot vary.
//
// On-disk vs wire. On-disk the revCv slot carries `revCv-or-hash` per row
// (Legacy rows = rawCv, Hashed rows = the 24-byte prefixed hash). On the wire the
// revCv slot is always rawCv: senders rebuild to Legacy form before emit
// (`BuildAttachmentKeyForExternalEmit`) and receivers reject
// Hashed-form input as a sender-side bug.
//
// Sender-side Legacy-form emit reads the parent revision's version-only CV
// from the row column (AttachmentsTable.RevisionVersion / TombstoneTable.RevisionVersion)
// and rebuilds the composite in Legacy form (mirrors the RT slice); throws on
// a missing column value for a Hashed-form row as a writer-side-invariant guard.
//
// Live writers (Put / Delete / CreateTombstone / DeleteTombstones) always
// persist new rows at the canonical hash-PK form with a delete-before-set
// guard against the raw-CV form so the two forms can't coexist for a single
// logical attachment.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Schemas.Attachments;
using static Raven.Server.Documents.Schemas.Tombstones;

namespace Raven.Server.Documents
{
    //   HashComposite   = [lowerDocId][RS]'r'[RS][24-byte prefixed hash][RS][lowerName][RS][attHash44][RS][lowerContentType]
    //   RawComposite    = [lowerDocId][RS]'r'[RS][rawCv             ][RS][lowerName][RS][attHash44][RS][lowerContentType]
    //   RevisionVersion = [rawCv]
    internal readonly struct RevisionAttachmentKey
    {
        public readonly Slice HashComposite;
        public readonly Slice RawComposite;
        public readonly Slice RevisionVersion;

        public RevisionAttachmentKey(Slice hashComposite, Slice rawComposite, Slice revisionVersion)
        {
            HashComposite = hashComposite;
            RawComposite = rawComposite;
            RevisionVersion = revisionVersion;
        }
    }

    // Owns the composite-slice allocations plus a stable copy of the parent-rev version-only CV
    // (so writers can safely add it to a TVB even after the source slice's scope exits).
    internal struct RevisionAttachmentKeyScope : IDisposable
    {
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _withHashScope;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _withRawCvScope;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _revisionVersionScope;

        internal RevisionAttachmentKeyScope(
            ByteStringContext<ByteStringMemoryCache>.InternalScope withHashScope,
            ByteStringContext<ByteStringMemoryCache>.InternalScope withRawCvScope,
            ByteStringContext<ByteStringMemoryCache>.InternalScope revisionVersionScope = default)
        {
            _withHashScope = withHashScope;
            _withRawCvScope = withRawCvScope;
            _revisionVersionScope = revisionVersionScope;
        }

        public void Dispose()
        {
            _revisionVersionScope.Dispose();
            _withRawCvScope.Dispose();
            _withHashScope.Dispose();
        }
    }

    public unsafe partial class AttachmentsStorage
    {
        internal static RevisionAttachmentKeyScope BuildRevisionAttachmentKey(
            DocumentsOperationContext context,
            in RevisionKey revisionKey,
            byte* lowerId, int lowerIdSize,
            byte* lowerName, int lowerNameSize,
            Slice base64Hash,
            byte* lowerContentTypePtr, int lowerContentTypeSize,
            out RevisionAttachmentKey pair)
        {
            return BuildPair(context, in revisionKey, (ctx, cvSegment, out keySlice) =>
                AttachmentKey.GetKey(ctx, lowerId, lowerIdSize, lowerName, lowerNameSize, base64Hash,
                    lowerContentTypePtr, lowerContentTypeSize, AttachmentType.Revision, cvSegment, out keySlice),
                out pair);
        }

        internal static RevisionAttachmentKeyScope BuildRevisionAttachmentPrefix(
            DocumentsOperationContext context,
            in RevisionKey revisionKey,
            Slice lowerId,
            out RevisionAttachmentKey pair)
        {
            return BuildPair(context, in revisionKey, (ctx, cvSegment, out keySlice) =>
                AttachmentKey.GetPrefix(ctx, lowerId, AttachmentType.Revision, cvSegment, out keySlice),
                out pair);
        }

        internal static RevisionAttachmentKeyScope BuildRevisionAttachmentPartialKey(
            DocumentsOperationContext context,
            in RevisionKey revisionKey,
            byte* lowerId, int lowerIdSize,
            byte* lowerName, int lowerNameSize,
            out RevisionAttachmentKey pair)
        {
            return BuildPair(context, in revisionKey, (ctx, cvSegment, out keySlice) =>
                AttachmentKey.GetKeyInternal(ctx, lowerId, lowerIdSize, lowerName, lowerNameSize, default(Slice), null, 0,
                    AttachmentKey.KeyType.PartialKey, AttachmentType.Revision, cvSegment, out keySlice),
                out pair);
        }

        internal static RevisionAttachmentKeyScope BuildRevisionAttachmentKeyFromComposite(
            DocumentsOperationContext context,
            in RevisionKey revisionKey,
            Slice sourceComposite,
            out RevisionAttachmentKey pair)
        {
            var parts = ParseAllParts(sourceComposite.AsReadOnlySpan());

            byte* src = sourceComposite.Content.Ptr;
            using (Slice.External(context.Allocator, src + parts.AttHashStart, parts.AttHashSize, out Slice base64Hash))
            {
                return BuildRevisionAttachmentKey(context, in revisionKey,
                    src, parts.DocIdSize,
                    src + parts.LowerNameStart, parts.LowerNameSize,
                    base64Hash,
                    src + parts.LowerContentTypeStart, parts.LowerContentTypeSize,
                    out pair);
            }
        }

        public static RevAttachmentParts ParseAllParts(ReadOnlySpan<byte> key)
        {
            ParseRevisionAttachmentKey(key,
                out int docIdSize,
                out int lowerNameStart, out int lowerNameSize,
                out int hashStart, out int hashSize,
                out int lowerContentTypeStart, out int lowerContentTypeSize);

            return new RevAttachmentParts(docIdSize,
                lowerNameStart, lowerNameSize,
                hashStart, hashSize,
                lowerContentTypeStart, lowerContentTypeSize);
        }

        public readonly struct RevAttachmentParts
        {
            public readonly int DocIdSize;
            public readonly int LowerNameStart, LowerNameSize;
            public readonly int AttHashStart, AttHashSize;
            public readonly int LowerContentTypeStart, LowerContentTypeSize;

            public int CvSegmentStart => DocIdSize + 3;
            public int CvSegmentSize => LowerNameStart - 1 - CvSegmentStart;

            public RevAttachmentParts(int docIdSize,
                int lowerNameStart, int lowerNameSize,
                int attHashStart, int attHashSize,
                int lowerContentTypeStart, int lowerContentTypeSize)
            {
                DocIdSize = docIdSize;
                LowerNameStart = lowerNameStart;
                LowerNameSize = lowerNameSize;
                AttHashStart = attHashStart;
                AttHashSize = attHashSize;
                LowerContentTypeStart = lowerContentTypeStart;
                LowerContentTypeSize = lowerContentTypeSize;
            }
        }

        private delegate ByteStringContext<ByteStringMemoryCache>.InternalScope KeyBuilder(DocumentsOperationContext context, Slice cvSegment, out Slice keySlice);

        private static RevisionAttachmentKeyScope BuildPair(
            DocumentsOperationContext context,
            in RevisionKey revisionKey,
            KeyBuilder build,
            out RevisionAttachmentKey pair)
        {
            var withHashScope = build(context, revisionKey.PrefixedHash, out Slice withHash);
            var withRawCvScope = build(context, revisionKey.Raw, out Slice withRawCv);
            // Stable copy of revisionKey.Raw owned by this scope -- caller's slice may dispose before the row write.
            var revisionVersionScope = Slice.From(context.Allocator, revisionKey.Raw.AsReadOnlySpan(), out Slice revisionVersion);
            pair = new RevisionAttachmentKey(withHash, withRawCv, revisionVersion);
            return new RevisionAttachmentKeyScope(withHashScope, withRawCvScope, revisionVersionScope);
        }

        // EMIT (revision attachment): rawCv from Attachment.RevisionVersion (Hashed rows) or the PK revCv segment (Legacy rows).
        internal RevisionAttachmentKeyScope BuildAttachmentRevisionKey(
            DocumentsOperationContext context, Attachment attachment, out Slice revisionAttachmentKey)
            => BuildAttachmentKeyForExternalEmit(context, attachment.Key, attachment.RevisionVersion, out revisionAttachmentKey);

        // Same as above for a revision-attachment tombstone. Caller must have already filtered to Type=Attachment + 'r' discriminator.
        internal RevisionAttachmentKeyScope BuildAttachmentRevisionTombstoneKey(
            DocumentsOperationContext context, Tombstone tombstone, out Slice revisionAttachmentKey)
            => BuildAttachmentKeyForExternalEmit(context, tombstone.LowerId, tombstone.RevisionVersion, out revisionAttachmentKey);

        // EMIT shared: rawCv from column (Hashed source) or from the PK revCv segment (Legacy source).
        private RevisionAttachmentKeyScope BuildAttachmentKeyForExternalEmit(
            DocumentsOperationContext context,
            LazyStringValue sourceComposite,
            string revisionVersion,
            out Slice revisionAttachmentKey)
        {
            if (sourceComposite == null || sourceComposite.Size == 0)
                throw new ArgumentException("Source composite must be non-empty.", nameof(sourceComposite));

            revisionVersion ??= ExtractRevCvSegment(sourceComposite);   // Legacy on-disk: revCv slot is already rawCv.

            using (Slice.External(context.Allocator, sourceComposite.Buffer, sourceComposite.Size, out Slice sourceSlice))
            using (RevisionsStorage.BuildRevisionKey(context, revisionVersion, out RevisionKey revisionKey))
            {
                RevisionAttachmentKeyScope scope = BuildRevisionAttachmentKeyFromComposite(context, in revisionKey, sourceSlice, out RevisionAttachmentKey pair);
                revisionAttachmentKey = pair.RawComposite;
                return scope;
            }
        }

        public static string ExtractRevCvSegment(LazyStringValue key)
        {
            ParseRevisionAttachmentKey(new ReadOnlySpan<byte>(key.Buffer, key.Size),
                out int docIdSize,
                out int lowerNameStart, out _,
                out _, out _,
                out _, out _);

            int cvSegmentStart = docIdSize + 3; // [RS]['r'][RS]
            int cvSegmentSize = lowerNameStart - 1 - cvSegmentStart;

            if (cvSegmentSize <= 0)
                throw new InvalidOperationException(
                    $"Source composite '{key}' has an empty revCv segment.");
            if (cvSegmentSize == RevisionsStorage.RevisionKeySize)
                throw new InvalidOperationException(
                    $"Hashed-form revision-attachment-shape composite '{key}' has no RevisionVersion column value. " +
                    "This indicates a writer-side bug -- every Hashed-form RA / RAT write must populate the RevisionVersion column.");

            return Encoding.UTF8.GetString(key.Buffer + cvSegmentStart, cvSegmentSize);
        }

        // Parses the revision-attachment composite shape `[lowerDocId][RS]'r'[RS][revCv-or-hash][RS][lowerName][RS][attHash44][RS][lowerContentType]`; throws if not a full key (callers dispatch by GetAttachmentType upstream).
        public static void ParseRevisionAttachmentKey(
            ReadOnlySpan<byte> key,
            out int docIdSize,
            out int lowerNameStart, out int lowerNameSize,
            out int hashStart, out int hashSize,
            out int lowerContentTypeStart, out int lowerContentTypeSize)
        {
            int firstRs = AttachmentKey.FindNextSeparator(key, 0);
            if (firstRs < 0 || firstRs + 3 > key.Length || key[firstRs + 1] != AttachmentKey.RevisionType || key[firstRs + 2] != AttachmentKey.RecordSeparator)
                ThrowNotRevisionAttachmentKey(key);

            docIdSize = firstRs;

            int cvSegmentStart = firstRs + 3;
            int cvSegmentEnd = AttachmentKey.FindNextSeparator(key, cvSegmentStart);
            if (cvSegmentEnd < 0)
                ThrowNotRevisionAttachmentKey(key);

            lowerNameStart = cvSegmentEnd + 1;
            int lowerNameEnd = AttachmentKey.FindNextSeparator(key, lowerNameStart);
            if (lowerNameEnd < 0)
                ThrowNotRevisionAttachmentKey(key);

            lowerNameSize = lowerNameEnd - lowerNameStart;

            hashStart = lowerNameEnd + 1;
            int hashEnd = AttachmentKey.FindNextSeparator(key, hashStart);
            if (hashEnd < 0)
                ThrowNotRevisionAttachmentKey(key);

            hashSize = hashEnd - hashStart;

            lowerContentTypeStart = hashEnd + 1;
            lowerContentTypeSize = key.Length - lowerContentTypeStart;
            if (lowerContentTypeSize < 0)
                ThrowNotRevisionAttachmentKey(key);
        }


        [DoesNotReturn]
        private static void ThrowNotRevisionAttachmentKey(ReadOnlySpan<byte> key)
        {
            throw new InvalidOperationException(
                $"Key '{Encoding.UTF8.GetString(key)}' is not a revision-attachment full key.");
        }

        internal bool TryReadRevisionAttachmentByKey(Table table, in RevisionAttachmentKey pair, out TableValueReader tvr)
            => _documentDatabase.DocumentsStorage.RevisionsStorage.DualForm.SeekOnePrimaryKeyPrefix(table, pair.HashComposite, pair.RawComposite, out tvr);

        internal void DeleteRevisionAttachmentByKey(Table table, in RevisionAttachmentKey pair)
            => _documentDatabase.DocumentsStorage.RevisionsStorage.DualForm.Delete(table, pair.HashComposite, pair.RawComposite);

        // No dedupe needed -- the write paths' delete-before-set guard keeps one form per logical attachment.
        internal IEnumerable<Attachment> GetRevisionAttachmentsByPrefix(DocumentsOperationContext context, Table table, RevisionAttachmentKey prefixes)
        {
            foreach (var sr in table.SeekByPrimaryKeyPrefix(prefixes.HashComposite, Slices.Empty, 0))
            {
                Attachment attachment = TableValueToAttachment(context, ref sr.Value.Reader);
                if (attachment != null)
                    yield return attachment;
            }

            if (_documentDatabase.DocumentsStorage.RevisionsStorage.DualForm.HashOnly)
                yield break;

            foreach (var sr in table.SeekByPrimaryKeyPrefix(prefixes.RawComposite, Slices.Empty, 0))
            {
                Attachment attachment = TableValueToAttachment(context, ref sr.Value.Reader);
                if (attachment != null)
                    yield return attachment;
            }
        }

        // RECEIVE builder. Wire revCv slot is always rawCv (sender-rebuilt); Hashed-form revCv is rejected as a sender-side bug. The parsed rawCv flows into the row column on the subsequent RA/RAT write.
        internal static RevisionAttachmentKeyScope BuildRevisionAttachmentKeyFromWire(
            DocumentsOperationContext context,
            Slice wireComposite,
            out RevisionAttachmentKey pair)
        {
            var parts = ParseAllParts(wireComposite.AsReadOnlySpan());
            if (parts.CvSegmentSize <= 0)
                throw new InvalidOperationException(
                    $"Wire composite '{wireComposite}' has an empty revCv segment.");

            if (parts.CvSegmentSize == RevisionsStorage.RevisionKeySize)
                throw new InvalidOperationException(
                    $"Wire composite '{wireComposite}' has a Hashed-form revCv segment ({parts.CvSegmentSize} bytes). " +
                    "Senders must rebuild to Legacy form (rawCv) before emit; this indicates a sender-side bug.");

            byte* src = wireComposite.Content.Ptr;
            using (Slice.External(context.Allocator, src + parts.CvSegmentStart, parts.CvSegmentSize, out Slice rawCvSlice))
            using (Slice.External(context.Allocator, src + parts.AttHashStart, parts.AttHashSize, out Slice base64Hash))
            using (RevisionsStorage.GetRevisionKeyHashSlice(context.Allocator, rawCvSlice, out Slice prefixedHashSlice))
            {
                var synthRevisionKey = new RevisionKey(prefixedHashSlice, rawCvSlice);

                return BuildRevisionAttachmentKey(context, in synthRevisionKey,
                    src, parts.DocIdSize,
                    src + parts.LowerNameStart, parts.LowerNameSize,
                    base64Hash,
                    src + parts.LowerContentTypeStart, parts.LowerContentTypeSize,
                    out pair);
            }
        }

        internal Attachment GetRevisionAttachmentByPair(DocumentsOperationContext context, in RevisionAttachmentKey pair)
        {
            Table table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);
            if (TryReadRevisionAttachmentByKey(table, in pair, out TableValueReader tvr) == false)
                return null;
            return TableValueToAttachment(context, ref tvr);
        }

        internal Tombstone GetRevisionAttachmentTombstoneByPair(DocumentsOperationContext context, in RevisionAttachmentKey pair)
        {
            Table tombstoneTable = context.Transaction.InnerTransaction.OpenTable(_documentDatabase.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);
            if (TryReadRevisionAttachmentTombstoneByKey(tombstoneTable, in pair, out TableValueReader tvr) == false)
                return null;
            Tombstone tombstone = TableValueToTombstone(context, ref tvr);
            Debug.Assert(tombstone.Type == Tombstone.TombstoneType.Attachment, "Tombstone must be of type attachment");
            return tombstone;
        }

        internal bool TryReadRevisionAttachmentTombstoneByKey(Table tombstoneTable, in RevisionAttachmentKey pair, out TableValueReader tvr)
            => _documentDatabase.DocumentsStorage.RevisionsStorage.DualForm.TryRead(tombstoneTable,
                pair.HashComposite, pair.RawComposite, out tvr);

        // Suppresses duplicate inserts when a logically-equivalent tombstone already exists under the opposite key form.
        internal bool VerifyRevisionAttachmentTombstoneExists(Table tombstoneTable, in RevisionAttachmentKey pair)
            => _documentDatabase.DocumentsStorage.RevisionsStorage.DualForm.Exists(tombstoneTable, 
                pair.HashComposite, pair.RawComposite);

        // LazyStringValue overload for the receive path; internalises the storage-key conversion.
        internal void PutRevisionAttachmentDirect(
            DocumentsOperationContext context, in RevisionAttachmentKey pair,
            LazyStringValue name, LazyStringValue contentType, Slice base64Hash, long size, RemoteAttachmentParameters remoteParams,
            string changeVector)
        {
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, name, out _, out Slice nameSlice))
            using (DocumentIdWorker.GetLowerIdSliceAndStorageKey(context, contentType, out _, out Slice contentTypeSlice))
            {
                PutRevisionAttachmentDirect(context, in pair, nameSlice, contentTypeSlice, base64Hash, size, remoteParams, changeVector);
            }
        }

        internal void PutRevisionAttachmentDirect(
            DocumentsOperationContext context, in RevisionAttachmentKey pair,
            Slice name, Slice contentType, Slice base64Hash, long size, RemoteAttachmentParameters remoteParams,
            string changeVector = null)
        {
            Debug.Assert(base64Hash.Size == 44, $"Hash size should be 44 but was: {base64Hash.Size}");

            long newEtag = _documentsStorage.GenerateNextEtag();
            if (string.IsNullOrEmpty(changeVector))
                changeVector = _documentsStorage.GetNewChangeVector(context, newEtag);
            Debug.Assert(changeVector != null);

            Table table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            // Delete-before-set across both live-row forms and either-form tombstone (born-clean skips raw).
            DeleteRevisionAttachmentByKey(table, in pair);
            DeleteRevisionAttachmentTombstones(context, in pair);

            using (Slice.From(context.Allocator, changeVector, out Slice changeVectorSlice))
            using (GetRemoteAttachmentParametersSlices(context.Allocator, remoteParams, out Slice identifierSlice, out RemoteAttachmentFlags flags, out long ticks))
            using (table.Allocate(out TableValueBuilder tvb))
            {
                tvb.Add(pair.HashComposite.Content.Ptr, pair.HashComposite.Size);                            // 0
                tvb.Add(Bits.SwapBytes(newEtag));                                                            // 1
                tvb.Add(name.Content.Ptr, name.Size);                                                        // 2
                tvb.Add(contentType.Content.Ptr, contentType.Size);                                          // 3
                tvb.Add(base64Hash.Content.Ptr, base64Hash.Size);                                            // 4
                tvb.Add(context.GetTransactionMarker());                                                     // 5
                tvb.Add(changeVectorSlice.Content.Ptr, changeVectorSlice.Size);                              // 6
                tvb.Add(size);                                                                               // 7 Size
                tvb.Add(Bits.SwapBytes((int)flags));                                                         // 8 Flags
                tvb.Add(ticks);                                                                               // 9 RemoteAt
                tvb.Add(identifierSlice.Content.Ptr, identifierSlice.Size);                                  // 10 Identifier
                tvb.Add(pair.RevisionVersion.Content.Ptr, pair.RevisionVersion.Size);                        // 11 AttachmentsTable.RevisionVersion
                table.Set(tvb);
            }

            _documentDatabase.Metrics.Attachments.PutsPerSec.MarkSingleThreaded(1);
        }

        // Phantom-delete branch (no live row) reuses an existing tombstone's etag in either form, for replication continuity.
        internal void DeleteRevisionAttachmentDirect(
            DocumentsOperationContext context, in RevisionAttachmentKey pair,
            string expectedChangeVector, string changeVector, long lastModifiedTicks)
        {
            Table table = context.Transaction.InnerTransaction.OpenTable(AttachmentsSchema, AttachmentsMetadataSlice);

            DualFormProbe dualForm = _documentDatabase.DocumentsStorage.RevisionsStorage.DualForm;
            bool found = dualForm.TryRead(table, pair.HashComposite, pair.RawComposite, out TableValueReader tvr);

            if (found == false)
            {
                if (expectedChangeVector != null)
                    throw new ConcurrencyException(
                        $"Revision attachment with key '{pair.HashComposite}' does not exist, " +
                        $"but delete was called with change vector '{expectedChangeVector}'. " +
                        "Optimistic concurrency violation, transaction will be aborted.")
                    {
                        ExpectedChangeVector = expectedChangeVector
                    };

                Table tombstoneTable = context.Transaction.InnerTransaction.OpenTable(_documentDatabase.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);
                long attachmentEtag;
                if (TryReadRevisionAttachmentTombstoneByKey(tombstoneTable, in pair, out TableValueReader existingTombstone))
                {
                    attachmentEtag = TableValueToEtag((int)TombstoneTable.Etag, ref existingTombstone);
                    tombstoneTable.Delete(existingTombstone.Id);
                }
                else
                {
                    attachmentEtag = _documentsStorage.GenerateNextEtagForReplicatedTombstoneMissingDocument(context);
                }

                CreateRevisionAttachmentTombstone(context, in pair, attachmentEtag, changeVector, lastModifiedTicks, DocumentFlags.None);
                return;
            }

            string currentChangeVector = TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref tvr);
            long etag = TableValueToEtag((int)AttachmentsTable.Etag, ref tvr);

            using (TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out Slice hash))
            {
                if (expectedChangeVector != null && ChangeVector.CompareVersion(currentChangeVector, expectedChangeVector, context) != 0)
                {
                    throw new ConcurrencyException(
                        $"Revision attachment with key '{pair.HashComposite}' has change vector '{currentChangeVector}', " +
                        $"but Delete was called with change vector '{expectedChangeVector}'. " +
                        "Optimistic concurrency violation, transaction will be aborted.")
                    {
                        ActualChangeVector = currentChangeVector,
                        ExpectedChangeVector = expectedChangeVector
                    };
                }

                CreateRevisionAttachmentTombstone(context, in pair, etag, changeVector, lastModifiedTicks, DocumentFlags.None);

                context.Transaction.CheckIfShouldDeleteAttachmentStream(hash);
            }

            DeleteRevisionAttachmentByKey(table, in pair);
        }

        // Idempotent insert -- attachment tombstones are immutable, so a same-logical existing tombstone wins.
        internal void CreateRevisionAttachmentTombstone(
            DocumentsOperationContext context, in RevisionAttachmentKey pair,
            long attachmentEtag, string changeVector, long lastModifiedTicks, DocumentFlags flags)
        {
            Table table = context.Transaction.InnerTransaction.OpenTable(_documentDatabase.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);

            if (VerifyRevisionAttachmentTombstoneExists(table, in pair))
                return;

            long newEtag = _documentsStorage.GenerateNextEtag();
            using (table.Allocate(out TableValueBuilder tvb))
            using (Slice.From(context.Allocator, changeVector, out Slice cv))
            {
                tvb.Add(pair.HashComposite.Content.Ptr, pair.HashComposite.Size);
                tvb.Add(Bits.SwapBytes(newEtag));
                tvb.Add(Bits.SwapBytes(attachmentEtag));
                tvb.Add(context.GetTransactionMarker());
                tvb.Add((byte)Tombstone.TombstoneType.Attachment);
                tvb.Add(null, 0);
                tvb.Add((int)flags);
                tvb.Add(cv.Content.Ptr, cv.Size);
                tvb.Add(lastModifiedTicks);
                tvb.Add(pair.RevisionVersion.Content.Ptr, pair.RevisionVersion.Size);                        // 9 TombstoneTable.RevisionVersion
                table.Insert(tvb);
            }
        }

        // Must run before writing a fresh live row so a stale tombstone in either form can't replicate as a spurious delete.
        internal void DeleteRevisionAttachmentTombstones(DocumentsOperationContext context, in RevisionAttachmentKey pair)
        {
            Table table = context.Transaction.InnerTransaction.OpenTable(_documentDatabase.DocumentsStorage.TombstonesSchema, AttachmentsTombstonesSlice);
            _documentDatabase.DocumentsStorage.RevisionsStorage.DualForm.Delete(table, pair.HashComposite, pair.RawComposite);
        }
    }
}
