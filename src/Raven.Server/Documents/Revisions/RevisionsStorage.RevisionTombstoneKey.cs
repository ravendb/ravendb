// RevisionsStorage RevisionTombstoneKey slice. Builds and resolves the
// `[lowerDocumentId][RS][revCv-or-hash]` composite PK of revision tombstones
// (the deleted revision's cv or its hash22), exposes the dual-form
// read/exists/delete helpers, and provides the writer that mirrors an
// external source's PK (replication / smuggler) and the Legacy-form sender
// helper for cross-version emit.
//
// Field 7 of a revision-tombstone row carries the tombstone's own canonical
// CV (passed separately into CreateTombstone), not the deleted revision's
// CV; the embedded `RevisionKey` is only there for convenience.

using System;
using System.Collections.Generic;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;
using static Raven.Server.Documents.DocumentsStorage;
using static Raven.Server.Documents.Schemas.Revisions;

namespace Raven.Server.Documents.Revisions
{
    // Composite PK pair for a revision-tombstone row.
    //   HashComposite    -- [lowerDocId][RS][revHash22]
    //   RawComposite     -- [lowerDocId][RS][revCv.Version] (legacy / wire form)
    //   RevisionVersion  -- the parent revision's cv.Version bytes. Written into
    //                       TombstoneTable.RevisionVersion (field 9) on every Hashed-form RT.
    //   DocIdSlice       -- the lowerDocId prefix.
    internal readonly struct RevisionTombstoneKey
    {
        public readonly RevisionKey RevisionKey;
        public readonly Slice HashComposite;
        public readonly Slice RawComposite;
        public readonly Slice RevisionVersion;
        public readonly Slice DocIdSlice;

        public RevisionTombstoneKey(
            in RevisionKey revisionKey, Slice hashComposite, Slice rawComposite,
            Slice revisionVersion, Slice docIdSlice)
        {
            RevisionKey = revisionKey;
            HashComposite = hashComposite;
            RawComposite = rawComposite;
            RevisionVersion = revisionVersion;
            DocIdSlice = docIdSlice;
        }
    }

    // Bundle returned by the LIVE factory: revision-table key, tombstone composite key, and the doc-id
    // slices the writers need (lowered for prefix scans / field 1, original-case storage form for field 4).
    // RECEIVE / EMIT factories don't surface this -- they have no original-case docId to begin with.
    // Owns the underlying byte-string allocation: one call to GetLowerIdSliceAndStorageKey produces both
    // slices in a single scope, disposed when this struct is disposed.
    internal readonly struct DocIdSlices : IDisposable
    {
        public readonly Slice LowerId;   // lowered docId -- prefix scans, RevisionsTable.LowerId (field 1)
        public readonly Slice Id;        // storage-key form -- RevisionsTable.Id (field 4)
        private readonly ByteStringContext<ByteStringMemoryCache>.InternalScope _scope;

        internal DocIdSlices(ByteStringContext allocator, string docId)
        {
            _scope = DocumentIdWorker.GetLowerIdSliceAndStorageKey(allocator, docId, out LowerId, out Id);
        }

        public void Dispose() => _scope.Dispose();
    }

    internal readonly struct RevisionKeys
    {
        public readonly RevisionKey Revision;
        public readonly RevisionTombstoneKey Tombstone;
        public readonly DocIdSlices Slices;

        public RevisionKeys(in RevisionKey revision, in RevisionTombstoneKey tombstone, in DocIdSlices slices)
        {
            Revision = revision;
            Tombstone = tombstone;
            Slices = slices;
        }
    }

    // Owns every allocation that backs a RevisionTombstoneKey. Created only through the static factories,
    // one per source -- the ctor itself does no work that can fail:
    //   * ForLiveWrite(cv, docId)        -- live path; builds the composite from pieces.
    //   * FromExternalComposite(key)     -- external-source receive (wire / smuggler dump).
    //   * ForExternalEmit(tombstone)     -- sender-side Legacy-form rebuild.
    // Each factory disposes the partially-built scope if building throws, so a throwing build never leaves
    // a half-constructed object behind. LIVE and RECEIVE share `BuildHashedKey`; RECEIVE and EMIT share
    // `ParseAndSliceComposite`.
    internal sealed unsafe class RevisionTombstoneKeyScope : IDisposable
    {
        private ByteStringContext _allocator;
        private List<IDisposable> _scopes;

        private Slice _docIdSlice;
        private Slice _trailingSlice;
        private Slice _sourceSlice;
        private int _trailingSize => _trailingSlice.Size;

        public RevisionTombstoneKey TombstoneKey { get; private set; }

        private RevisionTombstoneKeyScope(ByteStringContext allocator)
        {
            _allocator = allocator;
        }

        // LIVE write path. Caller-supplied lowerDocId slice is held by external reference (not owned by this scope).
        public static RevisionTombstoneKeyScope ForWrite(DocumentsOperationContext context, ChangeVector changeVector, Slice lowerDocId)
        {
            var scope = new RevisionTombstoneKeyScope(context.Allocator);
            try
            {
                scope.BuildLive(changeVector, lowerDocId);
            }
            catch
            {
                scope.Dispose();
                throw;
            }
            return scope;
        }

        // RECEIVE (replication wire / smuggler dump). `collection` is consulted only on the pre-6.0 docId-less branch (parent-revision probe).
        public static RevisionTombstoneKeyScope FromExternal(DocumentsOperationContext context, LazyStringValue compositeKey, string collection)
        {
            var scope = new RevisionTombstoneKeyScope(context.Allocator);
            try
            {
                scope.BuildFromExternalComposite(context, compositeKey, collection);
            }
            catch
            {
                scope.Dispose();
                throw;
            }
            return scope;
        }

        // EMIT (sender-side) Legacy-form rebuild from an on-disk Tombstone.
        public static RevisionTombstoneKeyScope ForExternal(DocumentsOperationContext context, Tombstone tombstone)
        {
            var scope = new RevisionTombstoneKeyScope(context.Allocator);
            try
            {
                scope.BuildForExternalEmit(tombstone);
            }
            catch
            {
                scope.Dispose();
                throw;
            }
            return scope;
        }

        // ---------------- build steps (factory-only) ----------------

        // LIVE: build [docId][RS][rawCv] via the shared BuildCompositeSlice; lowerDocId is owned by the caller.
        private void BuildLive(ChangeVector changeVector, Slice lowerDocId)
        {
            _docIdSlice = lowerDocId;
            Register(Slice.From(_allocator, changeVector.Version, out _trailingSlice));
            Register(BuildCompositeSlice(_trailingSlice, out _sourceSlice));   // [docId][RS][rawCv] = RawComposite
            BuildHashedKey();
        }

        // RECEIVE: trailing must be Legacy form (rawCv); Hashed-form trailing is rejected as a sender-side bug.
        private void BuildFromExternalComposite(DocumentsOperationContext context, LazyStringValue compositeKey, string collection)
        {
            ParseAndSliceComposite(compositeKey.Buffer, compositeKey.Size);
            if (_trailingSize == RevisionsStorage.RevisionKeyHashSize)
                throw new InvalidOperationException(
                    $"Revision-tombstone composite '{compositeKey}' has a Hashed-form trailing segment. " +
                    "External sources must deliver Legacy form (rawCv); this indicates a sender-side bug.");

            if (_docIdSlice.Size == 0)
                BuildDocIdLessLegacyKey(context, collection);   // pre-6.0 docId-less wire key -- RECEIVE-only
            else
                BuildHashedKey();
        }

        // EMIT: rawCv from Tombstone.RevisionVersion (Hashed rows) or the PK trailing (Legacy rows).
        private void BuildForExternalEmit(Tombstone tombstone)
        {
            ParseAndSliceComposite(tombstone.LowerId.Buffer, tombstone.LowerId.Size);
            BuildEmitKey(tombstone);
        }

        // ---------------- shared private helpers ----------------

        // External-slice docId / trailing / source from a buffer. Shared by RECEIVE + EMIT.
        private void ParseAndSliceComposite(byte* buffer, int size)
        {
            int rsIdx = new ReadOnlySpan<byte>(buffer, size).IndexOf((byte)SpecialChars.RecordSeparator);

            // Pre-6.0 peers (RevisionTombstonesWithId == false) emit a docId-less wire key (bare cv.Version, no [RS]); rsIdx == 0 (empty docId WITH separator) is malformed and stays rejected as before.
            if (rsIdx == -1)
            {
                SlicePre60DocIdLessComposite(buffer, size);
                return;
            }

            if (rsIdx < 1 || rsIdx >= size - 1)
                throw new InvalidOperationException(
                    $"Expected a revision-tombstone composite key `[lowerDocId][RS][revCv-or-hash]`, got '{System.Text.Encoding.UTF8.GetString(buffer, size)}'.");

            Register(Slice.External(_allocator, buffer,             rsIdx,         out _docIdSlice));
            Register(Slice.External(_allocator, buffer + rsIdx + 1, size - rsIdx - 1, out _trailingSlice));
            Register(Slice.External(_allocator, buffer,             size,          out _sourceSlice));
        }

        // Pre-6.0 docId-less revision tombstone: bare cv.Version is the whole key; docId is empty.
        private void SlicePre60DocIdLessComposite(byte* buffer, int size)
        {
            _docIdSlice = Slices.Empty;
            Register(Slice.External(_allocator, buffer, size, out _trailingSlice));
            Register(Slice.External(_allocator, buffer, size, out _sourceSlice));
        }

        // Hash the trailing into a RevisionKey; `hash` is also embedded in the composite by BuildHashedKey.
        private RevisionKey BuildRevisionKeyFromTrailing(out Slice hash)
        {
            Register(RevisionsStorage.GetRevisionKeyHashSlice(
                _allocator, _trailingSlice, out hash, out Slice prefixedHash));
            return new RevisionKey(hash, prefixedHash, _trailingSlice);
        }

        // [docId][RS][hash] key (LIVE + RECEIVE). Embeds the bare 22B hash (not prefixed), byte-identical to pre-Shape-D.
        private void BuildHashedKey()
        {
            RevisionKey revisionKey = BuildRevisionKeyFromTrailing(out Slice hash);
            Register(BuildCompositeSlice(hash, out Slice hashComposite));
            TombstoneKey = new RevisionTombstoneKey(
                revisionKey, hashComposite, _sourceSlice, _trailingSlice, _docIdSlice);
        }

        // Pre-6.0 docId-less RECEIVE: preserves bare-CV Legacy PK; recovers docId from the parent revision row (otherwise the count-tree update no-ops and drifts +1); throws on orphan -- accepting would corrupt the count tree, peer must upgrade to 6.0+ (RevisionTombstonesWithId).
        private void BuildDocIdLessLegacyKey(DocumentsOperationContext context, string collection)
        {
            RevisionKey revisionKey = BuildRevisionKeyFromTrailing(out _);

            DocumentsStorage documentsStorage = context.DocumentDatabase.DocumentsStorage;
            var collectionName = documentsStorage.ExtractCollectionName(context, collection);
            var table = documentsStorage.RevisionsStorage.EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
            if (documentsStorage.RevisionsStorage.TryReadRevision(table, revisionKey, out TableValueReader tvr) == false)
            {
                throw new InvalidOperationException(
                    "Received a revision tombstone in the pre-6.0 docId-less wire form (bare cv.Version) for a revision " +
                    "that is not on disk; the docId cannot be recovered and accepting the tombstone would corrupt the " +
                    "revision-count tree. Upgrade the source peer to RavenDB 6.0 or newer so revision tombstones carry " +
                    "the docId on the wire (RevisionTombstonesWithId).");
            }

            Register(TableValueToSlice(context, (int)RevisionsTable.LowerId, ref tvr, out _docIdSlice));

            TombstoneKey = new RevisionTombstoneKey(
                revisionKey,
                hashComposite:   _sourceSlice,   // bare cv.Version
                rawComposite:    _sourceSlice,   // bare cv.Version
                revisionVersion: Slices.Empty,
                docIdSlice:      _docIdSlice);
        }

        // EMIT: resolve rawCv into _trailingSlice (Hashed: from RevisionVersion column; Legacy: trailing already is rawCv), then build RawComposite.
        private void BuildEmitKey(Tombstone tombstone)
        {
            if (string.IsNullOrEmpty(tombstone.RevisionVersion) == false)
            {
                Register(Slice.From(_allocator, tombstone.RevisionVersion, out _trailingSlice));
            }
            else if (_trailingSize == RevisionsStorage.RevisionKeyHashSize)
            {
                throw new InvalidOperationException(
                    $"Hashed-form revision-tombstone '{tombstone.LowerId}' has no RevisionVersion column value. " +
                    "This indicates a writer-side bug -- every Hashed-form RT write must populate TombstoneTable.RevisionVersion.");
            }

            Register(BuildCompositeSlice(_trailingSlice, out Slice rawComposite));
            TombstoneKey = new RevisionTombstoneKey(default, default, rawComposite, _trailingSlice, _docIdSlice);
        }

        internal void Register(IDisposable scope)
        {
            _scopes ??= new List<IDisposable>(capacity: 4);
            _scopes.Add(scope);
        }

        public void Dispose()
        {
            if (_scopes == null) return;
            for (int i = _scopes.Count - 1; i >= 0; i--)
                _scopes[i].Dispose();
            _scopes = null;
        }

        // build the slice [lowerDocId][RS][revCv-or-hash]
        private ByteStringContext<ByteStringMemoryCache>.InternalScope BuildCompositeSlice(Slice tailSlice, out Slice composite)
        {
            if (_docIdSlice.Size == 0)
            {
                // docId-less legacy form: no [docId][RS] prefix -> bare trailing, byte-identical to pre-6.0.
                return Slice.From(_allocator, tailSlice.AsReadOnlySpan(), out composite);
            }

            int totalSize = _docIdSlice.Size + 1 + tailSlice.Size;
            Span<byte> scratch = totalSize <= 256 ? stackalloc byte[256] : new byte[totalSize];
            Span<byte> active = scratch.Slice(0, totalSize);

            _docIdSlice.AsReadOnlySpan().CopyTo(active);
            active[_docIdSlice.Size] = SpecialChars.RecordSeparator;
            new ReadOnlySpan<byte>(tailSlice.Content.Ptr, tailSlice.Size).CopyTo(active.Slice(_docIdSlice.Size + 1));

            return Slice.From(_allocator, active, out composite);
        }
    }

    public partial class RevisionsStorage
    {
        // Caller-facing API -- thin adapters over the RevisionTombstoneKeyScope factories that surface the built keys as out-params.

        internal static RevisionTombstoneKeyScope BuildRevisionKeys(
            DocumentsOperationContext context, ChangeVector changeVector, string docId,
            out RevisionKeys keys)
        {
            // DocIdSlices owns its byte-string allocation; we Register it on the tombstone scope so disposal unwinds both in LIFO order.
            var slices = new DocIdSlices(context.Allocator, docId);
            RevisionTombstoneKeyScope tombstoneScope;
            try
            {
                tombstoneScope = RevisionTombstoneKeyScope.ForWrite(context, changeVector, slices.LowerId);
            }
            catch
            {
                slices.Dispose();
                throw;
            }

            tombstoneScope.Register(slices);
            keys = new RevisionKeys(
                revision:  tombstoneScope.TombstoneKey.RevisionKey,
                tombstone: tombstoneScope.TombstoneKey,
                slices:    slices);
            return tombstoneScope;
        }

        internal RevisionTombstoneKeyScope BuildRevisionTombstoneKeyFromExternal(
            DocumentsOperationContext context, LazyStringValue compositeKey, string collection,
            out RevisionTombstoneKey tombstoneKey)
        {
            var scope = RevisionTombstoneKeyScope.FromExternal(context, compositeKey, collection);
            tombstoneKey = scope.TombstoneKey;
            return scope;
        }

        internal static RevisionTombstoneKeyScope BuildRevisionTombstoneKeyForExternal(
            DocumentsOperationContext context, Tombstone tombstone,
            out RevisionTombstoneKey tombstoneKey)
        {
            var scope = RevisionTombstoneKeyScope.ForExternal(context, tombstone);
            tombstoneKey = scope.TombstoneKey;
            return scope;
        }

        internal void WriteRevisionTombstoneFromReplication(DocumentsOperationContext context, RevisionTombstoneReplicationItem item, string tombstoneChangeVector)
            => WriteRevisionTombstoneFromExternal(
                context,
                compositeKey:          item.Id,
                collection:            item.Collection,
                tombstoneChangeVector: tombstoneChangeVector,
                lastModifiedTicks:     item.LastModifiedTicks,
                flags:                 item.Flags);

        internal void WriteRevisionTombstoneFromSmuggler(DocumentsOperationContext context, Tombstone tombstone)
            => WriteRevisionTombstoneFromExternal(
                context,
                compositeKey:          tombstone.LowerId,
                collection:            tombstone.Collection,
                tombstoneChangeVector: tombstone.ChangeVector,
                lastModifiedTicks:     tombstone.LastModified.Ticks,
                flags:                 tombstone.Flags);

        // Lands an external-source revision tombstone: factory parses composite -> canonical Hashed PK, dual-form parent-revision lookup for the parent etag, CreateTombstone for the new row.
        private void WriteRevisionTombstoneFromExternal(
            DocumentsOperationContext context,
            LazyStringValue compositeKey,
            string collection,
            string tombstoneChangeVector,
            long lastModifiedTicks,
            DocumentFlags flags)
        {
            using (BuildRevisionTombstoneKeyFromExternal(context, compositeKey, collection, out RevisionTombstoneKey tombstoneKey))
            {
                var collectionName = _documentsStorage.ExtractCollectionName(context, collection);
                var revisionsTable = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);

                var foundRevision = TryReadRevision(revisionsTable, in tombstoneKey.RevisionKey, out TableValueReader revTvr);

                long revisionEtag;
                if (foundRevision)
                {
                    revisionEtag = TableValueToEtag((int)RevisionsTable.Etag, ref revTvr);
                    revisionsTable.Delete(revTvr.Id);
                    using (GetKeyPrefix(context.Allocator, tombstoneKey.DocIdSlice, out Slice prefixSlice))
                        IncrementCountOfRevisions(context, prefixSlice, -1);
                }
                else
                {
                    revisionEtag = _documentsStorage.GenerateNextEtagForReplicatedTombstoneMissingDocument(context);
                }

                CreateTombstone(context, in tombstoneKey, revisionEtag, collectionName, tombstoneChangeVector, lastModifiedTicks, flags);
            }
        }

        internal bool TryReadRevisionTombstone(Table table, in RevisionTombstoneKey tombstoneKey, out TableValueReader tvr)
            => DualForm.TryRead(tombstoneKey.HashComposite, tombstoneKey.RawComposite, table.ReadByKey, out tvr);

        internal bool VerifyRevisionTombstoneExists(Table table, in RevisionTombstoneKey tombstoneKey)
            => DualForm.Exists(tombstoneKey.HashComposite, tombstoneKey.RawComposite, table.VerifyKeyExists);

        // Deletes both PK forms; DeleteByKey is a no-op on miss.
        internal void DeleteRevisionTombstoneByKey(Table table, in RevisionTombstoneKey tombstoneKey)
            => DualForm.Apply(tombstoneKey.HashComposite, tombstoneKey.RawComposite, table.DeleteByKey);

        // Extracts the document-id prefix from `[lowerDocId][RS][revCv-or-hash]`; the trailing segment is consumer-opaque.
        public static bool TryExtractDocumentIdFromRevisionTombstoneKey(LazyStringValue compositeKey, out string documentId)
        {
            int index = compositeKey.IndexOf((char)SpecialChars.RecordSeparator, StringComparison.OrdinalIgnoreCase);
            if (index == -1)
            {
                documentId = null;
                return false;
            }

            documentId = compositeKey.Substring(0, index);
            return true;
        }
    }
}
