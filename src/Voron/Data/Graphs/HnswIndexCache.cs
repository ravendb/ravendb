using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Impl;
using Voron.Util;

namespace Voron.Data.Graphs;

/// <summary>
/// Per-index in-memory cache of HNSW node topology backed by a single contiguous unmanaged
/// buffer. Each cached node is laid out as a fixed header followed by per-level offsets and
/// the flat edge list. Lookups go through a <see cref="ConcurrentDictionary{TKey,TValue}"/>
/// keyed by node id; the dict is the publication point for newly appended records and
/// provides the acquire load that lets readers observe the bytes a writer placed before
/// publishing.
///
/// The cache is initially populated by <see cref="WarmFromScratch"/> (BFS from the entry
/// point through every reachable node up to the configured budget) and incrementally
/// maintained by <see cref="ApplyCommit"/> after each indexing commit. Capacity is
/// provisioned at construction time; if the buffer fills, further <see cref="TryAppend"/>
/// calls fail gracefully and those nodes simply miss the cache.
/// </summary>
public sealed unsafe class HnswIndexCache : IDisposable
{
    public readonly Hnsw.Options Options;
    public readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> SimilarityCalc;

    private byte* _buffer;
    private long _writeHead;
    private readonly long _capacityBytes;
    private readonly ConcurrentDictionary<long, NodeRef> _ids;
    private int _disposed;

    public int Count => _ids.Count;
    public long CapacityBytes => _capacityBytes;
    public long UsedBytes => Volatile.Read(ref _writeHead);

    public HnswIndexCache(long capacityBytes, in Hnsw.Options options,
        delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> similarityCalc)
    {
        if (capacityBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacityBytes), capacityBytes, "capacity must be positive");

        _capacityBytes = capacityBytes;
        _buffer = (byte*)NativeMemory.AlignedAlloc((nuint)capacityBytes, 4096);
        if (_buffer == null)
            throw new OutOfMemoryException($"Failed to allocate {capacityBytes} bytes for the HNSW node cache.");
        _ids = new ConcurrentDictionary<long, NodeRef>();
        Options = options;
        SimilarityCalc = similarityCalc;
    }

    ~HnswIndexCache() => DisposeNative();

    public void Dispose()
    {
        DisposeNative();
        GC.SuppressFinalize(this);
    }

    private void DisposeNative()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;
        var ptr = _buffer;
        _buffer = null;
        if (ptr != null)
            NativeMemory.AlignedFree(ptr);
    }

    /// <summary>
    /// Reserve a 64-byte-aligned region of <paramref name="sizeBytes"/> in the buffer. Lock-free
    /// CAS on <see cref="_writeHead"/>; fails when the cap is reached.
    /// </summary>
    public bool TryAllocate(int sizeBytes, out long offset)
    {
        int aligned = AlignUp(sizeBytes, 64);
        long head, next;
        do
        {
            head = Volatile.Read(ref _writeHead);
            next = head + aligned;
            if (next > _capacityBytes)
            {
                offset = -1;
                return false;
            }
        } while (Interlocked.CompareExchange(ref _writeHead, next, head) != head);
        offset = head;
        return true;
    }

    /// <summary>
    /// Append a node record. The bytes are written first; the dict entry publishes them via the
    /// volatile store at the end of <see cref="ConcurrentDictionary{TKey,TValue}.TryAdd"/>.
    /// Returns false on capacity exhaustion or duplicate id.
    /// </summary>
    public bool TryAppend(long nodeId, in CachedNodeBuilder src)
    {
        Debug.Assert(src.LevelOffsets.Length == src.LevelCount + 1, "level offsets length mismatch");
        Debug.Assert(src.Edges.Length == src.EdgesTotalCount, "edges length mismatch");

        int totalBytes = sizeof(CachedNodeHeader)
                         + sizeof(int) * (src.LevelCount + 1)
                         + sizeof(long) * src.EdgesTotalCount;

        if (TryAllocate(totalBytes, out long offset) == false)
            return false;

        var h = (CachedNodeHeader*)(_buffer + offset);
        h->NodeId = nodeId;
        h->PostingListId = src.PostingListId;
        h->VectorId = src.VectorId;
        h->LevelCount = src.LevelCount;
        h->_padding0 = 0;
        h->_padding1 = 0;
        h->EdgesTotalCount = src.EdgesTotalCount;

        var offsets = (int*)(h + 1);
        src.LevelOffsets.CopyTo(new Span<int>(offsets, src.LevelCount + 1));

        var edges = (long*)(offsets + src.LevelCount + 1);
        src.Edges.CopyTo(new Span<long>(edges, src.EdgesTotalCount));

        // Linearization point: only after the bytes above are written do readers become able to
        // discover this record. ConcurrentDictionary does not document release semantics for the
        // bucket store, so on weakly-ordered architectures (ARM64) the buffer writes above and the
        // dictionary insert below could be observed out of order by a reader on another core. This
        // fence makes the buffer writes globally visible before the record is published; the reader
        // side gets an acquire load from the dictionary plus a control dependency on the offset.
        Interlocked.MemoryBarrier();
        return _ids.TryAdd(nodeId, new NodeRef(offset));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetNode(long nodeId, out CachedNodeView view)
    {
        if (_ids.TryGetValue(nodeId, out var r) == false)
        {
            view = default;
            return false;
        }
        var h = (CachedNodeHeader*)(_buffer + r.Offset);
        var offs = (int*)(h + 1);
        var edges = (long*)(offs + h->LevelCount + 1);
        view = new CachedNodeView(h, offs, edges);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ContainsNode(long nodeId) => _ids.ContainsKey(nodeId);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int AlignUp(int v, int align) => (v + align - 1) & ~(align - 1);

    private static delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> ResolveSimilarityKernel(in Hnsw.Options options) =>
        Hnsw.GetDistanceKernel(options);

    /// <summary>
    /// Conservative bytes-per-node estimate used to translate a node-count budget into a buffer
    /// size at construction time. Worst-case node has <c>2*numberOfEdges</c> entries at level 0
    /// across the flat edge list.
    /// </summary>
    public static int EstimateBytesPerNode(int numberOfEdges)
    {
        const int AvgLevelCount = 4;
        return sizeof(CachedNodeHeader)
               + sizeof(int) * (AvgLevelCount + 1)
               + sizeof(long) * 2 * numberOfEdges
               + 64;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    public struct CachedNodeHeader
    {
        public long NodeId;
        public long PostingListId;
        public long VectorId;
        public byte LevelCount;
        public byte _padding0;
        public short _padding1;
        public int EdgesTotalCount;
    }

    public readonly struct CachedNodeView
    {
        public readonly CachedNodeHeader* Header;
        public readonly int* Offsets;
        public readonly long* Edges;

        public CachedNodeView(CachedNodeHeader* header, int* offsets, long* edges)
        {
            Header = header;
            Offsets = offsets;
            Edges = edges;
        }

        public byte LevelCount => Header->LevelCount;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<long> EdgesAtLevel(int level)
        {
            int start = Offsets[level];
            int end = Offsets[level + 1];
            return new ReadOnlySpan<long>(Edges + start, end - start);
        }
    }

    /// <summary>
    /// Caller-built view of a node ready to be appended. The spans must remain stable for the
    /// duration of the <see cref="TryAppend"/> call.
    /// </summary>
    public ref struct CachedNodeBuilder
    {
        public long PostingListId;
        public long VectorId;
        public byte LevelCount;
        public int EdgesTotalCount;
        public ReadOnlySpan<int> LevelOffsets;
        public ReadOnlySpan<long> Edges;
    }

    private readonly record struct NodeRef(long Offset);

    /// <summary>
    /// Initial fill: BFS from the entry point. Upper levels (>= 1) are fully connected by HNSW
    /// construction so a single sweep per level suffices. Level 0 needs full BFS because under
    /// the standard HNSW level formula most nodes live only at level 0 and many sit &gt;1 hop
    /// from any upper-level seed, so a single hop would silently miss them. BFS allocation
    /// order means neighboring nodes become neighboring records in <see cref="_buffer"/>,
    /// giving Gorder-style locality for free.
    /// </summary>
    public static HnswIndexCache WarmFromScratch(LowLevelTransaction llt, Slice fieldName, int maxNodes)
    {
        var tree = llt.Transaction.ReadTree(fieldName);
        if (tree is null || tree.TryGetLookupFor(Hnsw.NodeIdToLocationSlice, out Lookup<Int64LookupKey> locations) == false)
            return null;

        var options = Unsafe.Read<Hnsw.Options>(tree.DirectRead(Hnsw.OptionsSlice));
        var simCalc = ResolveSimilarityKernel(options);

        long bytesPerNode = EstimateBytesPerNode(options.NumberOfEdges);
        long capacityBytes = (long)Math.Max(1, maxNodes) * bytesPerNode * 11 / 10;
        var cache = new HnswIndexCache(capacityBytes, options, simCalc);

        if (maxNodes <= 0 || locations.TryGetValue(Hnsw.EntryPointId, out _) == false)
            return cache;

        using var builder = new WarmupBuilder(llt, locations, maxNodes);
        builder.SeedAndLoad(Hnsw.EntryPointId);
        if (builder.LoadedCount == 0)
            return cache;

        int maxLevel = builder.NodeLevelsAt(0) - 1;
        for (int level = maxLevel; level > 0 && builder.HasBudget; level--)
            builder.ExpandAtLevel(level);

        // Level 0 needs full BFS, not a single hop: under the standard HNSW level formula
        // most nodes live only at level 0 and many sit >1 hop from any upper-level seed, so
        // one ExpandAtLevel(0) call leaves them out.
        while (builder.HasBudget)
        {
            int before = builder.LoadedCount;
            builder.ExpandAtLevel(0);
            if (builder.LoadedCount == before)
                break;
        }

        builder.AppendAllPromoted(cache);
        return cache;
    }

    /// <summary>
    /// Per-commit incremental update. Loads each dirty node from the just-committed snapshot
    /// and appends a record. <see cref="TryAppend"/> is a no-op for nodes already present
    /// (id collision), so re-touched nodes keep their previous cache entry.
    /// </summary>
    public void ApplyCommit(LowLevelTransaction llt, Slice fieldName, IEnumerable<long> dirtyNodeIds)
    {
        if (dirtyNodeIds is null)
            return;

        var tree = llt.Transaction.ReadTree(fieldName);
        if (tree is null || tree.TryGetLookupFor(Hnsw.NodeIdToLocationSlice, out Lookup<Int64LookupKey> locations) == false)
            return;

        using var builder = new WarmupBuilder(llt, locations, budget: int.MaxValue);
        foreach (var nodeId in dirtyNodeIds)
        {
            if (_ids.ContainsKey(nodeId))
                continue;
            builder.SeedAndLoad(nodeId);
        }
        builder.AppendAllPromoted(this);
    }

    /// <summary>
    /// BFS scratch that owns the temporary <see cref="NativeList"/>s. Emits node records in the
    /// order they were discovered, so a subsequent bulk append produces a locality-friendly
    /// layout in the cache buffer.
    /// </summary>
    private sealed class WarmupBuilder : IDisposable
    {
        private readonly LowLevelTransaction _llt;
        private readonly Lookup<Int64LookupKey> _locations;
        private readonly int _budget;
        private readonly Dictionary<long, int> _nodeIdToIdx = new();
        private NativeList<Hnsw.Node> _working;
        private NativeList<long> _batch;

        public bool HasBudget => _nodeIdToIdx.Count < _budget;
        public int LoadedCount => _nodeIdToIdx.Count;
        public int NodeLevelsAt(int index) => _working[index].EdgesPerLevel.Count;

        public WarmupBuilder(LowLevelTransaction llt, Lookup<Int64LookupKey> locations, int budget)
        {
            _llt = llt;
            _locations = locations;
            _budget = budget;
        }

        public void SeedAndLoad(long nodeId)
        {
            if (_nodeIdToIdx.ContainsKey(nodeId))
                return;
            _batch.Add(_llt.Allocator, nodeId);
            Flush();
        }

        /// <summary>
        /// Sweep at <paramref name="level"/>: every loaded node whose level-L edge list is
        /// non-empty contributes its neighbors to the next batch. Multiple passes are required
        /// because a Flush appends new nodes whose own level-L edges must also be followed
        /// before we drop down a level.
        /// </summary>
        public void ExpandAtLevel(int level)
        {
            var seen = new HashSet<long>();
            int cursor = 0;
            while (cursor < _working.Count && _nodeIdToIdx.Count + _batch.Count < _budget)
            {
                for (int i = cursor; i < _working.Count && _nodeIdToIdx.Count + _batch.Count < _budget; i++)
                {
                    ref var node = ref _working[i];
                    if (node.EdgesPerLevel.Count <= level)
                        continue;

                    ref var edges = ref node.EdgesPerLevel[level];
                    for (int e = 0; e < edges.Count; e++)
                    {
                        var edgeId = edges[e];
                        if (_nodeIdToIdx.ContainsKey(edgeId) || seen.Add(edgeId) == false)
                            continue;
                        _batch.Add(_llt.Allocator, edgeId);
                        if (_nodeIdToIdx.Count + _batch.Count >= _budget)
                            break;
                    }
                }
                cursor = _working.Count;

                if (_batch.Count == 0)
                    break;
                Flush();
            }
        }

        // Lookup<T>.GetFor walks pages left-to-right and updates LastSearchPosition, so unsorted
        // keys produce spurious not-found results. Sort here, then map decoded results back to
        // the original slot order so emit order matches insertion order.
        private void Flush()
        {
            var keys = _batch.ToSpan();
            int priorCount = _working.Count;

            var targetSlots = new int[keys.Length];
            _working.EnsureCapacityFor(_llt.Allocator, keys.Length);
            for (int i = 0; i < keys.Length; i++)
            {
                targetSlots[i] = _working.Count;
                _working.Add(_llt.Allocator, new Hnsw.Node { NodeId = keys[i] });
            }

            keys.Sort(targetSlots.AsSpan());
            _locations.GetFor(keys, keys, -1);

            var spans = new UnmanagedSpan[keys.Length];
            Container.GetAll(_llt, keys, spans.AsSpan(), -1, _llt.PageLocator);
            for (int i = 0; i < keys.Length; i++)
            {
                // Reachable from the post-commit hook: a throw here would block every later
                // cache update for the index. Drop the corrupt node; the query path surfaces it.
                if (spans[i].Length == 0)
                    continue;
                Hnsw.Node.Decode(_llt, spans[i].ToSpan()).LoadInto(ref _working[targetSlots[i]]);
            }

            for (int i = priorCount; i < _working.Count; i++)
            {
                ref var node = ref _working[i];
                int levels = node.EdgesPerLevel.Count;
                if (levels == 0 || levels > byte.MaxValue)
                    continue;
                _nodeIdToIdx[node.NodeId] = i;
            }

            _batch.Clear();
        }

        /// <summary>
        /// Bulk-emit every loaded node into <paramref name="cache"/>. Routers (level &gt;= 1)
        /// and leaves (level 0 only) are both admitted; the BFS in <see cref="WarmFromScratch"/>
        /// caps the loaded set at the configured budget, and the goal is the highest cache hit
        /// rate over the working set rather than a structural slice of the graph.
        /// </summary>
        public void AppendAllPromoted(HnswIndexCache cache)
        {
            Span<int> levelOffsetsScratch = stackalloc int[byte.MaxValue + 1];
            var edgesScratch = new List<long>(cache.Options.NumberOfEdges * 4);

            foreach (var (_, idx) in _nodeIdToIdx)
            {
                ref var node = ref _working[idx];
                int levels = node.EdgesPerLevel.Count;
                if (levels == 0)
                    continue; // tombstone / missing — skip

                edgesScratch.Clear();
                levelOffsetsScratch[0] = 0;
                for (int lvl = 0; lvl < levels; lvl++)
                {
                    var src = node.EdgesPerLevel[lvl];
                    for (int e = 0; e < src.Count; e++)
                        edgesScratch.Add(src[e]);
                    levelOffsetsScratch[lvl + 1] = edgesScratch.Count;
                }

                var builder = new CachedNodeBuilder
                {
                    PostingListId = node.PostingListId,
                    VectorId = node.VectorId,
                    LevelCount = (byte)levels,
                    EdgesTotalCount = edgesScratch.Count,
                    LevelOffsets = levelOffsetsScratch[..(levels + 1)],
                    Edges = CollectionsMarshal.AsSpan(edgesScratch),
                };
                if (cache.TryAppend(node.NodeId, builder) == false)
                    break; // capacity exhausted: subsequent nodes also fail, stop early
            }
        }

        public void Dispose()
        {
            _batch.Dispose(_llt.Allocator);
            _working.Dispose(_llt.Allocator);
        }
    }
}
