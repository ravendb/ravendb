// RevisionsStorage RevisionKey slice. Dual-form PK primitive (`RevisionKey`),
// dual-form probe gate (`DualFormProbe` / `DualForm`), canonical write helper
// (`WriteRevisionTableRow`), row-shape reader (`ReadChangeVectorFromTvr`),
// per-row argument struct (`RevisionsTableRow`). Hashing lives in the Hashing
// slice, revision-tombstone composites in the RevisionTombstoneKey slice.
//
// Dual-form PK. Revision rows are addressed by two PK shapes: the canonical
// hash form (`Blake2b-128(cv.Version)`, 22 bytes) and the legacy raw `cv.Version`
// bytes. Lookups probe hash first; the raw probe is skipped on born-clean
// databases (`HashedRevisionPk` token). The full canonical CV travels into the
// writer via `RevisionsTableRow.FullChangeVector` and is written to
// `RevisionsTable.FullChangeVector` (field 12) on Hashed-form rows. Legacy
// 12-field rows have no field 12; readers dispatch on `tvr.Count` and fall
// back to field 0 (version-only) -- see `ReadChangeVectorFromTvr`.
//
// Upgrade-on-touch: a Legacy row is migrated to the canonical Hashed PK as a
// side effect of any actual row rewrite (MarkRevisionAsConflicted's
// WriteRevisionTableRow does delete-both-forms + Set-at-hash with field 12
// populated from the incoming full canonical CV). A no-op re-Put leaves the
// Legacy row in place.

using System;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Schemas.Revisions;

namespace Raven.Server.Documents.Revisions
{
    // Variable parts of a revisions-table row -- everything except the key
    // (carried by RevisionKey) and SwappedLastModified (derived from LastModifiedTicks).
    // FullChangeVector carries the canonical "order|version" CV that lands in
    // RevisionsTable.FullChangeVector (field 12) on Hashed-form rows -- threaded
    // through here so all WriteRevisionTableRow call sites supply it uniformly.
    internal unsafe struct RevisionsTableRow
    {
        public long EtagSwapBytes;             // field 3 (already byte-swapped)
        public byte* DocumentPtr;
        public int DocumentSize;
        public DocumentFlags Flags;            // field 6
        public long DeletedEtagOrMarker;       // field 7 (NotDeletedRevisionMarker, swapped etag, etc.)
        public long LastModifiedTicks;         // field 8; field 11 is swap-bytes of this
        public short TransactionMarker;        // field 9
        public int ResolvedField;              // field 10 (raw int value, may be any DocumentFlags or 0)
        public string FullChangeVector;        // field 12; full canonical "order|version" CV
    }
    // LowerId / IdSlice intentionally NOT on RevisionsTableRow -- WriteRevisionTableRow takes them from
    // `in RevisionKeys keys` instead, since every writer has the keys bundle in scope already.

    // Primary-key pair for a revisions-table row (field 0). Not a composite -- the bytes below ARE the PK.
    // A Hashed row is stored under PrefixedHash; Raw is the Legacy form (dual-form fallback). Full formats:
    //
    //   PrefixedHash = [u16 etag-sum prefix (2B, 0x1E-safe)][base64 Blake2b-128(cv.Version) (22B)]   -- 24 bytes
    //   Raw          = [rawCv]                                                                       -- cv.Version bytes

    // The full canonical CV travels separately via RevisionsTableRow.FullChangeVector
    // (write side) and RevisionsTable.FullChangeVector (read side, on Hashed rows).
    internal readonly struct RevisionKey
    {
        public readonly Slice PrefixedHash;
        public readonly Slice Raw;

        public RevisionKey(Slice prefixedHash, Slice raw)
        {
            PrefixedHash = prefixedHash;
            Raw = raw;
        }
    }

    internal struct RevisionKeyScope : IDisposable
    {
        private ByteStringContext.InternalScope _rawScope;
        private RevisionKeyHashScope _hashScope;

        internal RevisionKeyScope(
            ByteStringContext.InternalScope rawScope,
            RevisionKeyHashScope hashScope)
        {
            _rawScope = rawScope;
            _hashScope = hashScope;
        }

        public void Dispose()
        {
            _hashScope.Dispose();
            _rawScope.Dispose();
        }
    }

    public readonly struct DualFormProbe(bool hashOnly)
    {
        public bool HashOnly => hashOnly;

        public bool TryRead(Table table, Slice hash, Slice raw, out TableValueReader result)
        {
            if (table.ReadByKey(hash, out result))
                return true;
            if (hashOnly)
                return false;
            return table.ReadByKey(raw, out result);
        }

        public bool SeekOnePrimaryKeyPrefix(Table table, Slice hash, Slice raw, out TableValueReader result)
        {
            if (table.SeekOnePrimaryKeyPrefix(hash, out result))
                return true;
            if (hashOnly)
                return false;
            return table.SeekOnePrimaryKeyPrefix(raw, out result);
        }

        public bool Exists(Table table, Slice hash, Slice raw)
        {
            if (table.VerifyKeyExists(hash))
                return true;
            if (hashOnly)
                return false;
            return table.VerifyKeyExists(raw);
        }

        public bool Delete(Table table, Slice hash, Slice raw)
        {
            var deleted = table.DeleteByKey(hash);
            if (hashOnly)
                return deleted;
            return table.DeleteByKey(raw);
        }
    }

    public partial class RevisionsStorage
    {
        internal static RevisionKeyScope BuildRevisionKey(ByteStringContext allocator, ChangeVector changeVector, out RevisionKey key, bool strict = true)
        {
            if (changeVector == null)
                throw new ArgumentNullException(nameof(changeVector));

            var rawScope = Slice.From(allocator, changeVector.Version, out Slice raw);
            var hashScope = GetRevisionKeyHashSlice(allocator, raw, out Slice prefixedHash, strict);
            key = new RevisionKey(prefixedHash, raw);
            return new RevisionKeyScope(rawScope, hashScope);
        }

        internal static RevisionKeyScope BuildRevisionKey(DocumentsOperationContext context, string changeVector, out RevisionKey key, bool strict = true)
        {
            return BuildRevisionKey(context.Allocator, context.GetChangeVector(changeVector), out key, strict);
        }

        // Constructed fresh on each access -- `_database.SupportedFeatures` is set after RevisionsStorage's ctor.
        public DualFormProbe DualForm => new(_database.SupportedFeatures.SupportedFeatureTypes.HashedRevisionPk);

        internal bool TryReadRevision(Table table, in RevisionKey key, out TableValueReader tvr)
            => DualForm.TryRead(table, key.PrefixedHash, key.Raw, out tvr);

        internal bool VerifyRevisionExists(Table table, in RevisionKey key)
            => DualForm.Exists(table, key.PrefixedHash, key.Raw);

        // Must run before any Insert/Set so a stale legacy-PK row can't coexist with a fresh hash-PK row.
        internal void DeleteRevisionByKey(Table table, in RevisionKey key)
            => DualForm.Delete(table, key.PrefixedHash, key.Raw);

        internal static unsafe ChangeVector ReadChangeVectorFromTvr(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            // Hashed-form rows (post-migration) carry the full canonical CV in field 12.
            // Legacy 12-field rows have no field 12; their field 0 holds version-only rawCv as v6.2 wrote it
            if (tvr.Count > (int)RevisionsTable.FullChangeVector)
                return TableValueToChangeVector(context, (int)RevisionsTable.FullChangeVector, ref tvr);

            byte* pkPtr = tvr.Read((int)RevisionsTable.RevisionPk, out int pkSize);
            return context.GetChangeVector(System.Text.Encoding.UTF8.GetString(pkPtr, pkSize));
        }

        internal unsafe void WriteRevisionTableRecord(DocumentsOperationContext context, Table table, in RevisionKeys keys, RevisionsTableRow row, bool isInsert)
        {
            if (row.FullChangeVector == null)
                throw new ArgumentException("RevisionsTableRow.FullChangeVector must be non-null -- the writer cannot populate RevisionsTable.FullChangeVector (field 12) without it.", nameof(row));

            using (Slice.From(context.Allocator, row.FullChangeVector, out Slice fullCvSlice))
            {
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(keys.Revision.PrefixedHash.Content.Ptr, keys.Revision.PrefixedHash.Size);       // 0  RevisionPk (24 bytes: [u16 BE etag-sum][bare hash])
                    tvb.Add(keys.Slices.LowerId);                                                           // 1
                    tvb.Add(SpecialChars.RecordSeparator);                                                  // 2
                    tvb.Add(row.EtagSwapBytes);                                                             // 3
                    tvb.Add(keys.Slices.Id);                                                                // 4
                    tvb.Add(row.DocumentPtr, row.DocumentSize);                                             // 5
                    tvb.Add((int)row.Flags);                                                                // 6
                    tvb.Add(row.DeletedEtagOrMarker);                                                       // 7
                    tvb.Add(row.LastModifiedTicks);                                                         // 8
                    tvb.Add(row.TransactionMarker);                                                         // 9
                    tvb.Add(row.ResolvedField);                                                             // 10
                    tvb.Add(Bits.SwapBytes(row.LastModifiedTicks));                                         // 11
                    tvb.Add(fullCvSlice.Content.Ptr, fullCvSlice.Size);                                     // 12 FullChangeVector
                    DeleteRevisionByKey(table, in keys.Revision);
                    if (isInsert)
                        table.Insert(tvb);
                    else
                        table.Set(tvb);
                }
            }
        }

    }
}
