using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron.Data.Containers;
using Voron.Global;
using Voron.Impl;
using Voron.Util;

namespace Voron.Data.Graphs;

public partial class Hnsw
{
    public unsafe struct Node
    {
        internal const long VectorIdMask = ~0xFFF;

        // Hot fields read by the search inner loop are colocated at the start of the struct so a
        // single cache line covers PostingListId (tombstone check), _vectorSpan (kernel input),
        // Visited (RW per candidate), and the QueryDistance memo. VectorId / NodeId follow to keep
        // line 0 saturated; the resolve-only fields (EdgesPerLevel / EdgesIndexesPerLevel /
        // Cached*) land on line 1.
        public long PostingListId;
        internal UnmanagedSpan _vectorSpan;
        public int Visited;
        public int QueryDistanceVersion;
        // Cached distance between this node and the current query vector. Valid only when
        // QueryDistanceVersion == SearchState._queryVectorVersion. SearchState bumps that
        // version whenever the query vector changes (see OnQueryVector), which in turn
        // invalidates this entry without touching the node.
        public float QueryDistanceValue;
        public long VectorId;
        public long NodeId;

        public NativeList<NativeList<long>> EdgesPerLevel;
        public NativeList<NativeList<int>> EdgesIndexesPerLevel;

        // When non-null, this node's edge data lives in the HnswIndexCache buffer rather than in
        // EdgesPerLevel. Set during search-side LoadNodeIndexes when the node hits the cache and
        // the alternative copy was elided. Indexing-side code paths never set this pointer and
        // continue to read/write EdgesPerLevel as before.
        internal HnswIndexCache.CachedNodeHeader* CachedHeader;
        internal int* CachedOffsets;
        internal long* CachedEdges;

        // Set when a search read only the header of the node: the encoded levels, in a page of the read transaction, that
        // a level of EdgesPerLevel is decoded from when the search first expands the node at that level.
        internal byte* EncodedLevels;
        internal int EncodedLevelsLength;
        internal int EncodedLevelCount;
        // A bit for each level of EdgesPerLevel that was decoded, while EncodedLevels is set.
        internal ulong DecodedLevels;

        // Set by every change to what Encode writes: a commit writes back only the nodes that have it.
        internal bool Modified;

        // Appends an edge at the level. An index list that was in sync gets the edge too; a stale one is cleared, so that equal counts
        // cannot make it look in sync.
        internal void AppendEdge(ByteStringContext allocator, int level, long edgeId, int edgeIndex)
        {
            ref var ids = ref EdgesPerLevel[level];
            bool hasIndexes = EdgesIndexesPerLevel.Count > level;
            bool inSync = hasIndexes && EdgesIndexesPerLevel[level].Count == ids.Count;
            ids.Add(allocator, edgeId);
            if (inSync)
                EdgesIndexesPerLevel[level].Add(allocator, edgeIndex);
            else if (hasIndexes)
                EdgesIndexesPerLevel[level].Clear();
            Modified = true;
        }

        // Replaces the edges at the level with the nodes at these indexes.
        internal void SetEdges(SearchState searchState, int level, ReadOnlySpan<int> edgeIndexes)
        {
            var allocator = searchState.Llt.Allocator;
            ref var ids = ref EdgesPerLevel[level];
            ids.ResetAndEnsureCapacity(allocator, edgeIndexes.Length);
            foreach (var edgeIndex in edgeIndexes)
                ids.AddUnsafe(searchState.GetNodeByIndex(edgeIndex).NodeId);

            // PERF: The index list is rewritten from the indexes, without a lookup in the node id table.
            if (EdgesIndexesPerLevel.Count > level)
                EdgesIndexesPerLevel[level].ResetAndCopyFrom(allocator, edgeIndexes);
            Modified = true;
        }

        internal void SetPostingListId(long postingListId)
        {
            if (PostingListId == postingListId)
                return;

            PostingListId = postingListId;
            Modified = true;
        }

        internal void SetVectorId(long vectorId)
        {
            VectorId = vectorId;
            Modified = true;
        }

        // Gives the node an edge list for each level below levels. A node that gets more levels is written back.
        internal void EnsureLevels(ByteStringContext allocator, int levels)
        {
            if (EdgesPerLevel.Count >= levels)
                return;

            EdgesPerLevel.SetCapacity(allocator, levels);
            Modified = true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int GetLevelCount() => CachedHeader != null ? CachedHeader->LevelCount : EncodedLevels != null ? EncodedLevelCount : EdgesPerLevel.Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal int EdgeCountAtLevel(int level)
        {
            Debug.Assert(EncodedLevels == null || (DecodedLevels & (1UL << level)) != 0, "The edges of the level were not decoded");
            if (CachedHeader != null)
                return CachedOffsets[level + 1] - CachedOffsets[level];
            return EdgesPerLevel[level].Count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ReadOnlySpan<long> EdgesAtLevel(int level)
        {
            Debug.Assert(EncodedLevels == null || (DecodedLevels & (1UL << level)) != 0, "The edges of the level were not decoded");
            if (CachedHeader != null)
            {
                int start = CachedOffsets[level];
                int end = CachedOffsets[level + 1];
                return new ReadOnlySpan<long>(CachedEdges + start, end - start);
            }
            return EdgesPerLevel[level].ToSpan();
        }

        public bool VectorLoaded => _vectorSpan.Length > 0;

        /// <summary>
        /// Returns the vector's memory address and length without triggering I/O.
        /// Returns false if the vector hasn't been loaded yet.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGetVectorAddress(out byte* address, out int length)
        {
            if (_vectorSpan.Length > 0)
            {
                address = _vectorSpan.Address;
                length = _vectorSpan.Length;
                return true;
            }

            address = null;
            length = 0;
            return false;
        }

        public long GetVectorContainerId()
        {
            if ((VectorId & Constants.Graphs.VectorStorage.VectorContainerInternalIndexer) == 0)
                return VectorId;

            return VectorId & VectorIdMask;
        }

        public static NodeReader Decode(LowLevelTransaction llt, long id)
        {
            var span = Container.GetReadOnly(llt, new ContainerEntryId(id));
            return Decode(llt, span);
        }

        public static NodeReader Decode(LowLevelTransaction llt, Span<byte> span)
        {
            var postingListId = VariableSizeEncoding.Read<long>(span, out var pos);
            var offset = pos;
            var vectorId = VariableSizeEncoding.Read<long>(span, out pos, offset);
            offset += pos;
            var countOfLevels = VariableSizeEncoding.Read<int>(span, out pos, offset);
            offset += pos;

            return new NodeReader(llt.Allocator, span[offset..]) { PostingListId = postingListId, VectorId = vectorId, CountOfLevels = countOfLevels };
        }

        public Span<byte> Encode(ref ContextBoundNativeList<byte> buffer)
        {
            int countOfLevels = EdgesPerLevel.Count;

            // posting list id, vector id, count of levels
            var maxSize = 3 * VariableSizeEncoding.MaximumSizeOf<long>();
            for (int i = 0; i < countOfLevels; i++)
            {
                maxSize += EdgesPerLevel[i].Count * VariableSizeEncoding.MaximumSizeOf<long>();
            }

            buffer.EnsureCapacityFor(maxSize);

            var bufferSpan = buffer.ToFullCapacitySpan();

            var pos = VariableSizeEncoding.Write(bufferSpan, PostingListId);
            pos += VariableSizeEncoding.Write(bufferSpan, VectorId, pos);
            pos += VariableSizeEncoding.Write(bufferSpan, countOfLevels, pos);

            for (int i = 0; i < countOfLevels; i++)
            {
                Span<long> span = EdgesPerLevel[i].ToSpan();
                int len = Sorting.SortAndRemoveDuplicates(span);
                span = span[..len];
                long prev = 0;
                pos += VariableSizeEncoding.Write(bufferSpan, span.Length, pos);
                for (int j = 0; j < span.Length; j++)
                {
                    var delta = span[j] - prev;
                    prev = span[j];
                    pos += VariableSizeEncoding.Write(bufferSpan, delta, pos);
                }
            }

            return bufferSpan[..pos];
        }

        public UnmanagedSpan GetVectorUnmanagedSpan(SearchState state)
        {
            if (_vectorSpan.Length > 0)
                return _vectorSpan;

            _vectorSpan = NodeReader.ReadVector(VectorId, in state);
            return _vectorSpan;
        }

        public Span<byte> GetVector(SearchState state)
        {
            return GetVectorUnmanagedSpan(state).ToSpan();
        }

        internal void SetVector(in Options options, UnmanagedSpan span)
        {
            if ((VectorId & Constants.Graphs.VectorStorage.VectorContainerInternalIndexer) == 0)
            {
                _vectorSpan = span;
                return;
            }

            var count = (byte)(VectorId >> 1);
            var offset = count * options.VectorSizeBytes;
            _vectorSpan = new UnmanagedSpan(span.Address + offset, options.VectorSizeBytes);
        }
    }
}
