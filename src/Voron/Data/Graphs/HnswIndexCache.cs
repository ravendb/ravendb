using System;
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
/// Per-index in-memory cache of HNSW node topology. Lookups are gated on
/// <c>PublishedAtTxId &lt;= readerTxId</c>: a reader never sees an entry stamped by a commit
/// it cannot see on disk. The backing buffer grows monotonically; superseded regions are
/// not reclaimed and further appends fail gracefully once full.
///
/// Hash collisions are resolved by 1-step linear probing into a secondary slot; if both
/// slots are held by other ids the insert is refused and the node is left uncached.
/// A per-slot even/odd Version protects the NodeId/Offset/PublishedAtTxId triple from
/// torn reads: writers bump the version to odd while mutating and to the next even when
/// stable; readers fail the lookup if the version changes around their field reads. This
/// keeps the cache lock-free with single-writer-per-cache semantics and an unbounded
/// number of concurrent readers.
/// </summary>
public sealed unsafe class HnswIndexCache : IDisposable
{
    public readonly Hnsw.Options Options;
    public readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> SimilarityCalc;

    private Slot* _slots;
    private readonly int _slotCount;
    private readonly int _slotMask;

    private byte* _buffer;
    private long _writeHead;
    private readonly long _capacityBytes;

    private int _published;

    private int _disposed;

    public int Count => Volatile.Read(ref _published);
    public long CapacityBytes => _capacityBytes;
    public long UsedBytes => Volatile.Read(ref _writeHead);
    public int SlotCount => _slotCount;

    public HnswIndexCache(long capacityBytes, int slotCount, in Hnsw.Options options,
        delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> similarityCalc)
    {
        if (capacityBytes <= 0)
            throw new ArgumentOutOfRangeException(nameof(capacityBytes), capacityBytes, "capacity must be positive");
        if (slotCount <= 0 || (slotCount & (slotCount - 1)) != 0)
            throw new ArgumentOutOfRangeException(nameof(slotCount), slotCount, "slot count must be a positive power of two");

        _capacityBytes = capacityBytes;
        _slotCount = slotCount;
        _slotMask = slotCount - 1;
        _buffer = (byte*)NativeMemory.AlignedAlloc((nuint)capacityBytes, 4096);
        if (_buffer == null)
            throw new OutOfMemoryException($"Failed to allocate {capacityBytes} bytes for the HNSW node cache.");
        _slots = (Slot*)NativeMemory.AlignedAlloc((nuint)(sizeof(Slot) * slotCount), 64);
        new Span<byte>(_slots, sizeof(Slot) * slotCount).Clear();

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
        var buf = _buffer; _buffer = null;
        var slots = _slots; _slots = null;
        if (buf != null) NativeMemory.AlignedFree(buf);
        if (slots != null) NativeMemory.AlignedFree(slots);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int SlotIndexFor(long nodeId) => Sparrow.Hashing.Mix(nodeId) & _slotMask;

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
    /// Returns false only when the buffer is full; once that happens, every subsequent
    /// TryAppend also fails so the caller should stop. Slot collisions (both probe slots
    /// occupied by other ids) are skipped silently — the node simply isn't cached and the
    /// call still returns true.
    /// </summary>
    public bool TryAppend(long nodeId, long publishAtTxId, in CachedNodeBuilder src)
    {
        Debug.Assert(publishAtTxId > 0, "publishAtTxId must be a real tx id");
        Debug.Assert(src.LevelOffsets.Length == src.LevelCount + 1, "level offsets length mismatch");
        Debug.Assert(src.Edges.Length == src.EdgesTotalCount, "edges length mismatch");

        // Refuse rather than evict when both probe slots are held by other ids: silent
        // eviction of a still-valid entry would break readers that found it cacheable a
        // moment ago, and the warm-up path can tolerate occasional uncached nodes.
        int chosen = PickAppendSlot(nodeId);
        if (chosen < 0)
            return true;

        int totalBytes = sizeof(CachedNodeHeader)
                         + sizeof(int) * (src.LevelCount + 1)
                         + sizeof(long) * src.EdgesTotalCount;
        if (TryAllocate(totalBytes, out long offset) == false)
            return false;

        WriteNodeBytes(offset, nodeId, in src);
        PublishSlot(chosen, nodeId, publishAtTxId, offset);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int PickAppendSlot(long nodeId)
    {
        int primary = SlotIndexFor(nodeId);
        if (SlotAcceptsNodeId(primary, nodeId))
            return primary;
        int secondary = (primary + 1) & _slotMask;
        if (SlotAcceptsNodeId(secondary, nodeId))
            return secondary;
        return -1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool SlotAcceptsNodeId(int slotIdx, long nodeId)
    {
        ref var slot = ref _slots[slotIdx];
        long pub = Volatile.Read(ref slot.PublishedAtTxId);
        return pub == 0 || Volatile.Read(ref slot.NodeId) == nodeId;
    }

    private void WriteNodeBytes(long offset, long nodeId, in CachedNodeBuilder src)
    {
        var h = (CachedNodeHeader*)(_buffer + offset);
        h->NodeId = nodeId;
        h->PostingListId = src.PostingListId;
        h->VectorId = src.VectorId;
        h->LevelCount = src.LevelCount;
        h->_padding0 = 0;
        h->_padding1 = 0;
        h->EdgesTotalCount = src.EdgesTotalCount;

        var offs = (int*)(h + 1);
        src.LevelOffsets.CopyTo(new Span<int>(offs, src.LevelCount + 1));
        var edges = (long*)(offs + src.LevelCount + 1);
        src.Edges.CopyTo(new Span<long>(edges, src.EdgesTotalCount));
    }

    // Seqlock write: bump Version to odd, release the field stores, bump to next even.
    // Readers observing the odd version (or a mismatch between their pre/post version reads)
    // bail. Only an empty→occupied transition counts against _published.
    private void PublishSlot(int slotIdx, long nodeId, long publishAtTxId, long offset)
    {
        ref var slot = ref _slots[slotIdx];
        bool wasResident = Volatile.Read(ref slot.PublishedAtTxId) != 0;

        long v = Volatile.Read(ref slot.Version);
        Volatile.Write(ref slot.Version, v + 1);
        Volatile.Write(ref slot.NodeId, nodeId);
        Volatile.Write(ref slot.Offset, offset);
        Volatile.Write(ref slot.PublishedAtTxId, publishAtTxId);
        Volatile.Write(ref slot.Version, v + 2);

        if (wasResident == false)
            Interlocked.Increment(ref _published);
    }

    /// <summary>
    /// Caller must invoke from the AfterCommit hook where no new reader can start: the
    /// release-store on PublishedAtTxId must be visible before any reader at the new tx
    /// can issue a lookup.
    /// </summary>
    public void Evict(long nodeId, long atTxId)
    {
        _ = atTxId;
        int primary = SlotIndexFor(nodeId);
        if (Volatile.Read(ref _slots[primary].NodeId) == nodeId)
        {
            EvictSlot(primary);
            return;
        }
        int secondary = (primary + 1) & _slotMask;
        if (Volatile.Read(ref _slots[secondary].NodeId) == nodeId)
            EvictSlot(secondary);
    }

    private void EvictSlot(int slotIdx)
    {
        ref var slot = ref _slots[slotIdx];
        long v = Volatile.Read(ref slot.Version);
        Volatile.Write(ref slot.Version, v + 1);

        bool wasResident = Volatile.Read(ref slot.PublishedAtTxId) != 0;
        Volatile.Write(ref slot.PublishedAtTxId, 0);

        Volatile.Write(ref slot.Version, v + 2);

        if (wasResident)
            Interlocked.Decrement(ref _published);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetNode(long nodeId, long readerTxId, out CachedNodeView view)
    {
        int primary = SlotIndexFor(nodeId);
        if (TryReadSlot(primary, nodeId, readerTxId, out view))
            return true;
        return TryReadSlot((primary + 1) & _slotMask, nodeId, readerTxId, out view);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryReadSlot(int slotIdx, long nodeId, long readerTxId, out CachedNodeView view)
    {
        ref var slot = ref _slots[slotIdx];

        // Seqlock read: capture version, observe fields, re-read version. If the writer
        // raced through this slot the two version reads disagree (or the first is odd),
        // and we return a clean miss — the caller will fall back to disk.
        long v1 = Volatile.Read(ref slot.Version);
        if ((v1 & 1) != 0)
        {
            view = default;
            return false;
        }

        long ts = Volatile.Read(ref slot.PublishedAtTxId);
        long slotNodeId = Volatile.Read(ref slot.NodeId);
        long offset = Volatile.Read(ref slot.Offset);

        long v2 = Volatile.Read(ref slot.Version);
        if (v1 != v2 || ts == 0 || ts > readerTxId || slotNodeId != nodeId)
        {
            view = default;
            return false;
        }

        var h = (CachedNodeHeader*)(_buffer + offset);
        var offs = (int*)(h + 1);
        var edges = (long*)(offs + h->LevelCount + 1);
        view = new CachedNodeView(h, offs, edges);
        return true;
    }

    public void ForEachNode(long readerTxId, Action<long, CachedNodeView> visitor)
    {
        for (int i = 0; i < _slotCount; i++)
        {
            ref var slot = ref _slots[i];

            long v1 = Volatile.Read(ref slot.Version);
            if ((v1 & 1) != 0) continue;

            long ts = Volatile.Read(ref slot.PublishedAtTxId);
            long nodeId = Volatile.Read(ref slot.NodeId);
            long offset = Volatile.Read(ref slot.Offset);

            long v2 = Volatile.Read(ref slot.Version);
            if (v1 != v2 || ts == 0 || ts > readerTxId || nodeId == 0) continue;

            var h = (CachedNodeHeader*)(_buffer + offset);
            var offs = (int*)(h + 1);
            var edges = (long*)(offs + h->LevelCount + 1);
            visitor(nodeId, new CachedNodeView(h, offs, edges));
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int AlignUp(int v, int align) => (v + align - 1) & ~(align - 1);

    private static delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> ResolveSimilarityKernel(in Hnsw.Options options) =>
        Hnsw.GetDistanceKernel(options);

    public static int EstimateBytesPerNode(int numberOfEdges)
    {
        const int AvgLevelCount = 4;
        return sizeof(CachedNodeHeader)
               + sizeof(int) * (AvgLevelCount + 1)
               + sizeof(long) * 2 * numberOfEdges
               + 64;
    }

    public static int SlotCountFor(int nodeBudget) =>
        nodeBudget <= 0 ? 1 : Sparrow.Binary.Bits.PowerOf2(nodeBudget * 2);

    [StructLayout(LayoutKind.Sequential, Pack = 8)]
    internal struct Slot
    {
        public long Version;           // seqlock: even = stable, odd = writer in progress
        public long NodeId;            // 0 = never written
        public long PublishedAtTxId;   // 0 = evicted; the visibility gate
        public long Offset;
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

    public ref struct CachedNodeBuilder
    {
        public long PostingListId;
        public long VectorId;
        public byte LevelCount;
        public int EdgesTotalCount;
        public ReadOnlySpan<int> LevelOffsets;
        public ReadOnlySpan<long> Edges;
    }

    public static HnswIndexCache WarmFromScratch(LowLevelTransaction llt, Slice fieldName, int maxNodes)
    {
        var tree = llt.Transaction.ReadTree(fieldName);
        if (tree is null || tree.TryGetLookupFor(Hnsw.NodeIdToLocationSlice, out Lookup<Int64LookupKey> locations) == false)
            return null;

        var options = Unsafe.Read<Hnsw.Options>(tree.DirectRead(Hnsw.OptionsSlice));
        var simCalc = ResolveSimilarityKernel(options);

        long bytesPerNode = EstimateBytesPerNode(options.NumberOfEdges);
        long capacityBytes = (long)Math.Max(1, maxNodes) * bytesPerNode;
        int slotCount = SlotCountFor(Math.Max(1, maxNodes));
        var cache = new HnswIndexCache(capacityBytes, slotCount, options, simCalc);

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

        builder.AppendAll(cache, llt.Id);
        return cache;
    }

    public void ApplyCommit(LowLevelTransaction llt, Slice fieldName, IEnumerable<long> dirtyNodeIds)
    {
        if (dirtyNodeIds is null)
            return;

        var tree = llt.Transaction.ReadTree(fieldName);
        if (tree is null || tree.TryGetLookupFor(Hnsw.NodeIdToLocationSlice, out Lookup<Int64LookupKey> locations) == false)
            return;

        long txId = llt.Id;

        using var builder = new WarmupBuilder(llt, locations, budget: int.MaxValue);
        foreach (var nodeId in dirtyNodeIds)
        {
            Evict(nodeId, txId);
            builder.SeedAndLoad(nodeId);
        }
        builder.AppendAll(this, txId);
    }

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

        public void AppendAll(HnswIndexCache cache, long publishAtTxId)
        {
            Span<int> levelOffsetsScratch = stackalloc int[byte.MaxValue + 1];
            var edgesScratch = new List<long>(cache.Options.NumberOfEdges * 4);

            foreach (var (_, idx) in _nodeIdToIdx)
            {
                ref var node = ref _working[idx];
                int levels = node.EdgesPerLevel.Count;
                if (levels == 0)
                    continue;

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
                if (cache.TryAppend(node.NodeId, publishAtTxId, builder) == false)
                    break;
            }
        }

        public void Dispose()
        {
            _batch.Dispose(_llt.Allocator);
            _working.Dispose(_llt.Allocator);
        }
    }
}
