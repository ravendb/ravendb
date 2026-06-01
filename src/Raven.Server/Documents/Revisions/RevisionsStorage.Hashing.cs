// RevisionsStorage hashing slice. Produces two sibling slice views into one
// 24-byte allocation:
//   bareHashSlice     -- 22B: base64(Blake2b-128(canonical-identity-bytes)), unpadded.
//                        Embedded in composite PKs (RT/RA/RAT). Base64 (not raw) because
//                        composite segments are delimited by 0x1E and raw hash output
//                        would carry it with ~6% probability.
//   prefixedHashSlice -- 24B: [u16 BE etag-sum prefix][bare hash]. Revisions-table PK.
//                        Prefix is sum(entry.Etag) mod 2^16 over cv.Version entries --
//                        rule A like the hash. Stored as raw BE bytes (opaque to Voron
//                        at this site, no 0x1E hazard); BE is mandatory for lex == numeric
//                        ordering and the resulting B-tree locality.
//
// Tag-blind, order-canonical hash input: the bare-hash bytes hashed here come from
// fixed-width [8B BE etag][22B base64 dbId ASCII] entries sorted by (DbId, Etag) ordinal
// asc -- no separators, no tag prefix. The wire-form tag (A:, SINK:, TRXN:, ...) is
// replication annotation, not identity: two CVs that agree on (DbId, Etag) hash
// identically regardless of which tag the sender used. This frees the receiver from
// coordinating with the writer's retag rules (ReplaceKnownSinkEntries /
// ReplaceUnknownEntriesWithSinkTag).

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Raven.Server.Documents.Replication;
using Sparrow.Platform;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Revisions
{
    // Owns the three scopes backing the dual-output GetRevisionKeyHashSlice:
    // one InternalScope for the 24B buffer, two ExternalScopes for the [0..24)
    // and [2..24) slice views. Disposed LIFO.
    internal readonly struct RevisionKeyHashScope : IDisposable
    {
        private readonly ByteStringContext.InternalScope _bufferScope;
        private readonly ByteStringContext.ExternalScope _bareHashScope;
        private readonly ByteStringContext.ExternalScope _prefixedHashScope;

        internal RevisionKeyHashScope(
            ByteStringContext.InternalScope bufferScope,
            ByteStringContext.ExternalScope bareHashScope,
            ByteStringContext.ExternalScope prefixedHashScope)
        {
            _bufferScope = bufferScope;
            _bareHashScope = bareHashScope;
            _prefixedHashScope = prefixedHashScope;
        }

        public void Dispose()
        {
            _prefixedHashScope.Dispose();
            _bareHashScope.Dispose();
            _bufferScope.Dispose();
        }
    }

    public partial class RevisionsStorage
    {
        internal const int RevisionKeyHashSize = 22;

        private const int Blake2bHashSize = 16;
        private const int Base64PaddedSize = 24;

        internal const int EtagSumPrefixRawSize = 2;
        internal const int RevisionKeySize = EtagSumPrefixRawSize + RevisionKeyHashSize;

        // Per-entry identity bytes: [8B BE etag][22B base64 dbId ASCII] = 30B; 256B threshold = 8 entries on stack.
        private const int DbIdAsciiSize = 22;
        private const int IdentityBytesPerEntry = sizeof(long) + DbIdAsciiSize;
        private const int IdentityStackThreshold = 256;

        private static void EncodeHashToBase64(ReadOnlySpan<byte> rawHash, Span<byte> destination)
        {
            Debug.Assert(rawHash.Length == Blake2bHashSize);
            Debug.Assert(destination.Length >= RevisionKeyHashSize);

            Span<byte> padded = stackalloc byte[Base64PaddedSize];
            OperationStatus status = Base64.EncodeToUtf8(rawHash, padded, out _, out int written);
            Debug.Assert(status == OperationStatus.Done && written == Base64PaddedSize,
                $"Base64.EncodeToUtf8 returned status={status}, written={written}; expected Done/{Base64PaddedSize}.");
            padded.Slice(0, RevisionKeyHashSize).CopyTo(destination);
        }

        // Parse cv.Version into entries. strict=true throws on empty/malformed (write paths must fail loud); strict=false returns null and downstream short-circuits to a guaranteed-miss PK.
        private static List<ChangeVectorEntry> ParseVersionEntries(ReadOnlySpan<byte> versionBytes, bool strict)
        {
            string version = Encoding.UTF8.GetString(versionBytes);
            try
            {
                List<ChangeVectorEntry> entries = version.ToChangeVectorList();
                if (entries == null && strict)
                    throw new ArgumentException("Cannot build a revision key from an empty change vector.");
                return entries;
            }
            catch when (strict == false)
            {
                return null;
            }
        }

        // Sorts entries by (DbId, Etag) ordinal asc, packs [8B BE etag][22B base64 dbId ASCII] per entry, returns the un-modulo'd etag sum (caller applies the u16 mask for the PK prefix).
        private static int WriteVersionIdentityBytes(List<ChangeVectorEntry> entries, Span<byte> destination, out long etagSum)
        {
            entries.Sort();
            etagSum = 0L;
            int written = 0;
            for (int i = 0; i < entries.Count; i++)
            {
                ChangeVectorEntry entry = entries[i];
                etagSum += entry.Etag;

                BinaryPrimitives.WriteInt64BigEndian(destination[written..], entry.Etag);
                written += sizeof(long);

                written += Encoding.ASCII.GetBytes(entry.DbId, destination[written..]);
            }

            return written;
        }

        // Tag-blind Blake2b-128 -> 22B base64 + running etag sum, in one pass over the sorted entries. Stack scratch for <=8 entries; ArrayPool<byte>.Shared for larger CVs. Empty/null entries leave destination untouched (guaranteed lookup miss).
        internal static unsafe void ComputeBareRevisionHash(List<ChangeVectorEntry> entries, Span<byte> destination, out long etagSum)
        {
            Debug.Assert(destination.Length >= RevisionKeyHashSize);

            if (entries == null || entries.Count == 0)
            {
                etagSum = 0;
                return;
            }

            int upper = entries.Count * IdentityBytesPerEntry;

            if (upper <= IdentityStackThreshold)
            {
                Span<byte> scratch = stackalloc byte[IdentityStackThreshold];
                int len = WriteVersionIdentityBytes(entries, scratch, out etagSum);
                HashAndBase64(scratch[..len], destination);
                return;
            }

            byte[] rented = ArrayPool<byte>.Shared.Rent(upper);
            try
            {
                Span<byte> scratch = rented.AsSpan(0, upper);
                int len = WriteVersionIdentityBytes(entries, scratch, out etagSum);
                HashAndBase64(scratch[..len], destination);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        private static unsafe void HashAndBase64(ReadOnlySpan<byte> identityBytes, Span<byte> destination)
        {
            Span<byte> rawHash = stackalloc byte[Blake2bHashSize];
            Sodium.GenericHash16(identityBytes, rawHash);
            EncodeHashToBase64(rawHash, destination);
        }

        // Two External slice views over one 24B alloc: bareHashSlice [2..24) for composites; prefixedHashSlice [0..24) = [u16 BE etag-sum][bare hash] for the revisions-table PK (never enter a composite -- raw BE bytes can carry 0x1E).
        internal static unsafe RevisionKeyHashScope GetRevisionKeyHashSlice(
            ByteStringContext allocator,
            Slice rawChangeVectorSlice,
            out Slice bareHashSlice,
            out Slice prefixedHashSlice,
            bool strict = true)
        {
            ReadOnlySpan<byte> versionBytes = new ReadOnlySpan<byte>(
                rawChangeVectorSlice.Content.Ptr, rawChangeVectorSlice.Size);

            // One parse, one walk: the bare-hash write also yields the etag sum used by the prefix.
            List<ChangeVectorEntry> entries = ParseVersionEntries(versionBytes, strict);

            ByteStringContext.InternalScope bufferScope = allocator.Allocate(RevisionKeySize, out ByteString buf);
            try
            {
                Span<byte> prefixedSpan = new Span<byte>(buf.Ptr, RevisionKeySize);

                // Hash first into [2..24); ALSO returns the etag sum used by the prefix below.
                ComputeBareRevisionHash(entries, prefixedSpan[EtagSumPrefixRawSize..], out long etagPrefixSum);

                // Prefix at [0..2): u16 BE etag-sum. BE is mandatory (lex order == numeric order -> B-tree locality).
                BinaryPrimitives.WriteUInt16BigEndian(prefixedSpan, (ushort)(etagPrefixSum & 0xFFFF));

                ByteStringContext.ExternalScope prefixedHashScope =
                    Slice.External(allocator, buf.Ptr, RevisionKeySize, out prefixedHashSlice);
                ByteStringContext.ExternalScope bareHashScope =
                    Slice.External(allocator, buf.Ptr + EtagSumPrefixRawSize, RevisionKeyHashSize, out bareHashSlice);

                return new RevisionKeyHashScope(bufferScope, bareHashScope, prefixedHashScope);
            }
            catch
            {
                bufferScope.Dispose();
                throw;
            }
        }

    }
}
