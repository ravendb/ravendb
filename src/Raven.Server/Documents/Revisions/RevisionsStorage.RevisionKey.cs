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
    // LowerId / IdSlice intentionally NOT on RevisionsTableRow -- WriteRevisionTableRow takes them from `in RevisionKeys keys`.

    // Primary-key form of a revisions-table row.
    //   Hash         -- 22-byte ASCII base64 Blake2b-128(cv.Version). Bytes embedded as the cv
    //                   segment in every composite (RT / RA / RAT).
    //   PrefixedHash -- 24-byte [u16 BE etag-sum prefix][bare hash]. Used ONLY as the revisions-table PK
    //                   (reads, writes, deletes, existence checks). Must not enter a composite -- the raw
    //                   prefix bytes would re-introduce the 0x1E hazard. The bare-hash bytes at [2..24)
    //                   are byte-identical to `Hash`; both slices are External views into one buffer.
    //   Raw          -- legacy cv.Version bytes (variable size). Dual-form fallback for legacy rows
    //                   in the revisions table and as the rawCv segment in legacy composites.
    //
    // The full canonical CV travels separately via RevisionsTableRow.FullChangeVector
    // (write side) and RevisionsTable.FullChangeVector (read side, on Hashed rows).
    internal readonly struct RevisionKey
    {
        public readonly Slice Hash;
        public readonly Slice PrefixedHash;
        public readonly Slice Raw;

        public RevisionKey(Slice hash, Slice prefixedHash, Slice raw)
        {
            Hash = hash;
            PrefixedHash = prefixedHash;
            Raw = raw;
        }
    }

    internal readonly struct RevisionKeyScope : IDisposable
    {
        private readonly ByteStringContext.InternalScope _rawScope;
        private readonly RevisionKeyHashScope _hashScope;

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

    public readonly struct DualFormProbe
    {
        private readonly bool _hashOnly;

        public DualFormProbe(bool hashOnly)
        {
            _hashOnly = hashOnly;
        }

        public bool HashOnly => _hashOnly;

        public delegate bool DualReader<T>(Slice key, out T result);

        public bool TryRead<T>(Slice hash, Slice raw, DualReader<T> read, out T result)
        {
            if (read(hash, out result))
                return true;
            if (_hashOnly)
                return false;
            return read(raw, out result);
        }

        public bool Exists(Slice hash, Slice raw, Func<Slice, bool> exists)
        {
            if (exists(hash))
                return true;
            if (_hashOnly)
                return false;
            return exists(raw);
        }

        public void Apply(Slice hash, Slice raw, Func<Slice, bool> apply)
        {
            apply(hash);
            if (_hashOnly)
                return;
            apply(raw);
        }
    }

    public partial class RevisionsStorage
    {
        internal static RevisionKeyScope BuildRevisionKey(ByteStringContext allocator, ChangeVector changeVector, out RevisionKey key, bool strict = true)
        {
            if (changeVector == null)
                throw new ArgumentNullException(nameof(changeVector));

            var rawScope = Slice.From(allocator, changeVector.Version, out Slice raw);
            var hashScope = GetRevisionKeyHashSlice(allocator, raw, out Slice hash, out Slice prefixedHash, strict);
            key = new RevisionKey(hash, prefixedHash, raw);
            return new RevisionKeyScope(rawScope, hashScope);
        }

        internal static RevisionKeyScope BuildRevisionKey(DocumentsOperationContext context, string changeVector, out RevisionKey key, bool strict = true)
        {
            return BuildRevisionKey(context.Allocator, context.GetChangeVector(changeVector), out key, strict);
        }

        // Constructed fresh on each access -- `_database.SupportedFeatures` is set after RevisionsStorage's ctor.
        public DualFormProbe DualForm =>
            new DualFormProbe(_database.SupportedFeatures.SupportedFeatureTypes.HashedRevisionPk);

        internal bool TryReadRevision(Table table, in RevisionKey key, out TableValueReader tvr)
            => DualForm.TryRead(key.PrefixedHash, key.Raw, table.ReadByKey, out tvr);

        internal bool VerifyRevisionExists(Table table, in RevisionKey key)
            => DualForm.Exists(key.PrefixedHash, key.Raw, table.VerifyKeyExists);

        // Must run before any Insert/Set so a stale legacy-PK row can't coexist with a fresh hash-PK row.
        internal void DeleteRevisionByKey(Table table, in RevisionKey key)
            => DualForm.Apply(key.PrefixedHash, key.Raw, table.DeleteByKey);

        internal static unsafe ChangeVector ReadChangeVectorFromTvr(DocumentsOperationContext context, ref TableValueReader tvr)
        {
            // Hashed rows: field 12 = full canonical CV. Legacy 12-field rows have no field 12; field 0 holds version-only rawCv (v6.2 shape).
            if (tvr.Count > (int)RevisionsTable.FullChangeVector)
                return TableValueToChangeVector(context, (int)RevisionsTable.FullChangeVector, ref tvr);

            byte* pkPtr = tvr.Read((int)RevisionsTable.RevisionPk, out int pkSize);
            return context.GetChangeVector(System.Text.Encoding.UTF8.GetString(pkPtr, pkSize));
        }

        internal unsafe void WriteRevisionTableRow(DocumentsOperationContext context, Table table, in RevisionKeys keys, RevisionsTableRow row, bool isInsert)
        {
            if (row.FullChangeVector == null)
                throw new ArgumentException("RevisionsTableRow.FullChangeVector must be non-null -- the writer cannot populate RevisionsTable.FullChangeVector (field 12) without it.", nameof(row));

            using (Slice.From(context.Allocator, row.FullChangeVector, out Slice fullCvSlice))
            {
                using (table.Allocate(out TableValueBuilder tvb))
                {
                    tvb.Add(keys.Revision.PrefixedHash.Content.Ptr, keys.Revision.PrefixedHash.Size);       // 0  RevisionPk (24 bytes: [u16 BE etag-sum][bare hash])
                    tvb.Add(keys.Slices.LowerId);                                                          // 1
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
