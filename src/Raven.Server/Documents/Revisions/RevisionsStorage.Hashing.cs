// RevisionsStorage hashing slice. Produces one slice view into one 24-byte allocation:
//   prefixedHashSlice -- 24B: base64([u16 BE etag-sum][Blake2b-128(canonical-identity-bytes)]).
//                        The 18-byte input ([2B sum][16B hash]) base64-encodes to exactly 24 chars
//                        (no padding). Used both as the revisions-table PK and embedded as the cv slot
//                        of every composite PK (RT/RA/RAT). base64 (not raw bytes) is what makes the
//                        whole key composite-safe: composite segments are delimited by 0x1E and the
//                        base64 alphabet contains no 0x1E, so neither the hash nor the etag-sum prefix
//                        can split a segment. The 2-byte prefix is sum(entry.Etag) over cv.Version
//                        entries (rule A like the hash), written big-endian so the leading base64 chars
//                        grow with the sum -- clustering bulk writes into a small number of hot B-tree
//                        regions. (base64 is not strictly order-preserving, so this is locality, not a
//                        total order; the sum is write-and-forget and never decoded from disk.)
//
// Tag-blind, order-canonical hash input: the bare-hash bytes hashed here come from fixed-width
// [8B BE etag][22B base64 dbId ASCII] entries sorted by (DbId, Etag) ordinal asc -- no separators,
// no tag prefix. The wire-form tag (A:, SINK:, TRXN:, ...) is replication annotation, not identity:
// two CVs that agree on (DbId, Etag) hash identically regardless of which tag the sender used. This
// frees the receiver from coordinating with the writer's retag rules (ReplaceKnownSinkEntries /
// ReplaceUnknownEntriesWithSinkTag).

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Server.Documents.Replication;
using Sparrow.Platform;
using Sparrow.Server;
using Voron;
using Base64 = System.Buffers.Text.Base64;

namespace Raven.Server.Documents.Revisions
{
    // Owns the two scopes backing GetRevisionKeyHashSlice: one InternalScope for the 24B buffer
    // and one ExternalScope for the [0..24) prefixed-hash slice view. Disposed LIFO.
    internal struct RevisionKeyHashScope : IDisposable
    {
        private ByteStringContext.InternalScope _bufferScope;
        private ByteStringContext.ExternalScope _prefixedHashScope;

        internal RevisionKeyHashScope(
            ByteStringContext.InternalScope bufferScope,
            ByteStringContext.ExternalScope prefixedHashScope)
        {
            _bufferScope = bufferScope;
            _prefixedHashScope = prefixedHashScope;
        }

        public void Dispose()
        {
            _prefixedHashScope.Dispose();
            _bufferScope.Dispose();
        }
    }

    public partial class RevisionsStorage
    {
        private const int Blake2bHashSize = 16;
        internal const int RevisionKeyHashSize = 22;
        internal const int EtagSumPrefixRawSize = 2;
        internal const int RevisionKeySize = EtagSumPrefixRawSize + RevisionKeyHashSize;

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

        // One External slice view over a 24B alloc: prefixedHashSlice [0..24) = base64([u16 BE etag-sum][Blake2b-128]).
        // The whole key is base64 (0x1E-free), so it is used both as the revisions-table PK and embedded as the cv slot of every composite (RT/RA/RAT).
        internal static unsafe RevisionKeyHashScope GetRevisionKeyHashSlice(
            ByteStringContext allocator,
            Slice rawChangeVectorSlice,
            out Slice prefixedHashSlice,
            bool strict = true)
        {
            ReadOnlySpan<byte> versionBytes = rawChangeVectorSlice.AsReadOnlySpan();

            var entries = ParseVersionEntries(versionBytes, strict);
            if (entries == null)
            {
                prefixedHashSlice = Slices.Empty;
                return default;
            }

            ByteStringContext.InternalScope bufferScope = allocator.Allocate(RevisionKeySize, out ByteString buf);
            try
            {
                Span<byte> dest = buf.ToSpan();
               
                var cryptoGenericHashStateBytes = (int)Sodium.crypto_generichash_statebytes();
                byte* state = stackalloc byte[cryptoGenericHashStateBytes];
                if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, Blake2bHashSize) != 0)
                    ComputeHttpEtags.ThrowFailToInitHash();

                var sum = 0L;
                foreach (var entry in entries.Order())
                {
                    sum += entry.Etag;
                    ComputeHttpEtags.HashNumber(state, entry.Etag);
                    ComputeHttpEtags.HashChangeVector(state, entry.DbId);
                }

                var hash = stackalloc byte[Blake2bHashSize + EtagSumPrefixRawSize];
                Span<byte> hashSpan = new(hash, Blake2bHashSize + EtagSumPrefixRawSize);
                BinaryPrimitives.WriteUInt16BigEndian(hashSpan, (ushort)sum);
                
                if (Sodium.crypto_generichash_final(state, hash + sizeof(short), Blake2bHashSize) != 0)
                    ComputeHttpEtags.ThrowFailedToUpdateHash();

                OperationStatus status = Base64.EncodeToUtf8(hashSpan, dest, out _, out int written);
                Debug.Assert(status == OperationStatus.Done && written == RevisionKeySize,
                    $"Base64.EncodeToUtf8 returned status={status}, written={written}; expected Done/{RevisionKeySize}.");

                ByteStringContext.ExternalScope prefixedHashScope =
                    Slice.External(allocator, buf.Ptr, RevisionKeySize, out prefixedHashSlice);

                return new RevisionKeyHashScope(bufferScope, prefixedHashScope);
            }
            catch
            {
                bufferScope.Dispose();
                throw;
            }
        }
    }
}
