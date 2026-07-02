using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron.Util;

namespace Voron.Data.RoaringBitmaps;

/// <summary>
/// A roaring bitmap implementation optimized for Corax's native memory model.
/// All memory is allocated through ByteStringContext, ensuring zero-managed heap allocations
/// for the bitmap data. Non-negative values are split into container key (value &gt;&gt; 16)
/// and low 16 bits. Container lookup is O(1) via a flat index array sized to the max key.
///
/// Container types:
/// - Range: contiguous values start..start+count-1 (no data allocation). count=65536 means full.
///   Sequential Add at either range edge is O(1). Created automatically for contiguous inserts.
/// - ArrayUnsorted: append-only ushort[]. Add is O(1). Sorted lazily on first read.
/// - Array: sorted ushort[] for sparse data (cardinality &lt;= 4096, up to 8KB)
/// - Bitmap: 8KB fixed bitmap (1024 longs) for dense data (&gt; 4096 values)
///
/// Threading and consumption model:
/// - Single-threaded by design: no locking, no atomic state. Callers must not share a bitmap across threads.
/// - Set operations (<see cref="AndWith"/>, <see cref="AndNotWith"/>, <see cref="LazyOrWith"/>) are
///   intentionally destructive on their right-hand argument (containers stolen/sorted/mutated in place) to
///   skip a copy in hot paths. After being passed as the right side, a bitmap is consumed and must not be
///   read or used again until <see cref="Clear"/>'d — which also recycles its storage for reuse as scratch.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public unsafe partial struct RoaringBitmap(ByteStringContext ctx) : IDisposable
{
    internal NativeList<ContainerEntry> _entries;
    internal NativeList<ContainerType> _types;
    internal NativeList<int> _index;
    internal int _containerCount;
    /// <summary>
    /// Head of the entry-free list, 1-based: 0 = empty (ensure zero-init is default state), n = real slot index (n-1).
    /// </summary>
    internal int _containersFreeListHead;
    private FreeListHeads _buffersFreeListHeads;

    
    private bool _disposed;

#if DEBUG
    /// <summary>Set after this bitmap is passed as the right-hand side of a destructive
    /// set operation (OrWith, AndWith, AndNotWith). Any subsequent access asserts.
    /// Reset by <see cref="Clear"/>.</summary>
    private bool _consumed;
#endif

    [Conditional("DEBUG")]
    private readonly void AssertNotConsumed()
    {
#if DEBUG
        Debug.Assert(!_consumed, "Bitmap was consumed by a prior set operation. Call Clear() to reuse.");
#endif
    }

    [Conditional("DEBUG")]
    private static void MarkConsumed(ref RoaringBitmap bitmap)
    {
#if DEBUG
        bitmap._consumed = true;
#endif
    }

    private const int BitmapContainerSizeInBytes = 8192; // 8KB
    public const int BitmapContainerSizeInUInt64 = BitmapContainerSizeInBytes / sizeof(ulong);
    private const int ArrayContainerMaxCardinality = BitmapContainerSizeInBytes / sizeof(ushort); // crossover: array at max costs same as bitmap
    public const int ContainerKeyShift = 16;
    public const int ContainerSize = 1 << ContainerKeyShift; // 65,536 entry IDs per container
    private const int ContainerValueMask = 0xFFFF;
    private const int LazyCardinality = -1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int ResolveCardinality(ref ContainerEntry entry)
    {
        int card = entry.Cardinality;
        if (card == LazyCardinality)
        {
            card = BitmapContainerCardinality(entry.Data);
            entry.Cardinality = card;
        }
        return card;
    }

    private const int SimdAlignment = 32; // Vector256 width in bytes
    private const int SimdLinearScanThreshold = 64; // below this, SIMD linear scan beats binary/quad search

    private const int IndexAbsent = -1;        // key is not present in index

    [DoesNotReturn]

    private static void ThrowNegativeNotSupported(long value) => throw new ArgumentOutOfRangeException(nameof(value), value, "RoaringBitmap only supports non-negative values.");

    public readonly int ContainerCount => _containerCount;

   /// <summary>Total cardinality across ALL containers. Not cached, computed each time, avoid calling in hot loops:repairs any lazy bitmap (Cardinality == -1) along the way.</summary>
    public long ComputeCount()
    {
        AssertNotConsumed();
        long total = 0;
        ContainerEntry* entries = _entries.RawItems;
        ContainerType* types = _types.RawItems;
        int count = _entries.Count;
        for (int i = 0; i < count; i++)
        {
            if (types[i] == ContainerType.Free)
                continue;

            int card = entries[i].Cardinality;

            if (card is LazyCardinality) // LazyCardinality: bitmap container was updated without recomputing the popcount.
            {
                Debug.Assert(types[i] is ContainerType.Bitmap, "only bitmaps can have lazy cardinality");
                entries[i].Cardinality = card = BitmapContainerCardinality(entries[i].Data);
                if (card is 0)
                {
                    FreeContainer(entries[i].Key, i);
                    continue;
                }
            }
            total += card;
        }
        return total;
    }

    public readonly bool IsEmpty => _containerCount == 0;

    public readonly bool Contains(long value)
    {
        AssertNotConsumed();
        long key = value >> ContainerKeyShift;
        int slot = GetSlotForKey(key);
        if (slot < 0)
            return false;
        return ContainsAtSlot(slot, (ushort)(value & ContainerValueMask));
    }


    private readonly bool ContainsAtSlot(int slot, ushort low)
    {
        ref ContainerEntry entry = ref _entries[slot];
        ContainerType type = _types.RawItems[slot];
        return type switch
        {
            ContainerType.Array => ArrayContainerContains(entry.ArrayData, entry.Cardinality, low),
            ContainerType.ArrayUnsorted => SimdLinearContains(entry.ArrayData, entry.Cardinality, low),
            ContainerType.Bitmap => BitmapContains((ulong*)entry.Data, low),
            ContainerType.Range => low >= entry.RangeStart && low < entry.RangeStart + entry.Cardinality,
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unexpected container type")
        };
    }

    public readonly long MinContainerKey
    {
        get
        {
            if (_containerCount == 0)
                return -1;
            for (int i = 0; i < _index.Count; i++)
            {
                if (_index[i] != IndexAbsent)
                    return i;
            }
            return -1;
        }
    }

    public readonly long MaxContainerKey
    {
        get
        {
            if (_containerCount == 0)
                return -1;
            for (int i = _index.Count - 1; i >= 0; i--)
            {
                if (_index[i] != IndexAbsent)
                    return i;
            }
            return -1;
        }
    }

    private readonly int GetSlotForKey(long key)
    {
        if (key < 0 || key >= _index.Count)
            return IndexAbsent;
        return _index.RawItems[key];
    }

    /// <summary>
    /// Reset all containers, memory goes into _bitmap_ free list, not the allocator
    /// </summary>
    public void Clear()
    {
#if DEBUG
        _consumed = false;
#endif
        int count = _entries.Count;
        if (count == 0 && _index.Count == 0)
            return;

        ContainerEntry* entries = _entries.RawItems;
        ContainerType* types = _types.RawItems;

       for (int i = 0; i < count; i++)
        {
            if (types[i] != ContainerType.Free && entries[i].Storage.HasValue)
                _buffersFreeListHeads.Return(entries[i].Storage);
        }

        _entries.Clear();
        _types.Clear();
        _containerCount = 0;

        _containersFreeListHead = 0; // 0 = empty (entries are gone, free list is empty too)

        int* indexRaw = _index.RawItems;
        int indexLen = _index.Count;
        new Span<int>(indexRaw, indexLen).Fill(IndexAbsent);
    }

    /// <summary>Optimal bulk insert for known sorted data</summary>
    public void AddRange(ReadOnlySpan<long> sortedValues)
    {
        AssertNotConsumed();
        if (sortedValues.IsEmpty)
            return;

        AssertSorted(sortedValues);

        if (sortedValues[0] < 0)
            ThrowNegativeNotSupported(sortedValues[0]);

        int index = 0;
        while (index < sortedValues.Length)
        {
            long value = sortedValues[index];
            long key = value >> ContainerKeyShift;

            int start = index;
            // here we search for the index on the value in sortedValues that is the last that match
            // the current container, so we can bulk insert it
            index = SearchForContainerRangeEnd(sortedValues, (key + 1) << ContainerKeyShift, index + 1);
            ReadOnlySpan<long> containerValues = sortedValues.Slice(start, index - start);

            int slot = GetSlotForKey(key);
            if (slot >= 0)
            {
                // Existing container — batch add via AddRangeToContainer
                AddRangeToContainer(slot, containerValues);
                continue;
            }

            // New container — keep contiguous runs as a range (any start offset)
            ushort firstLow = (ushort)(containerValues[0] & ContainerValueMask);
            ushort lastLow = (ushort)(containerValues[^1] & ContainerValueMask);
            bool isRange = lastLow - firstLow == containerValues.Length - 1;

            if (isRange)
            {
                AddNewContainer(key, ContainerType.Range, new ContainerEntry
                {
                    Cardinality = containerValues.Length,
                    RangeStart = firstLow,
                    Storage = default
                });
                continue;
            }

            if (containerValues.Length > ArrayContainerMaxCardinality)
            {
                CreateBitmapContainerFromSorted(key, containerValues);
            }
            else
            {
                CreateArrayContainerFromSorted(key, containerValues);
            }
        }
    }

    private static int SearchForContainerRangeEnd(ReadOnlySpan<long> sortedValues, long nextKeyStart, int start)
    {
        int jump = 1;
        while (start + jump < sortedValues.Length && sortedValues[start + jump] < nextKeyStart)
            jump <<= 1; // Start with exponential jumps, then binary search within the last interval.
        int lo = start + (jump >> 1);
        int hi = Math.Min(start + jump, sortedValues.Length);
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (sortedValues[mid] < nextKeyStart)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }

    private void CreateBitmapContainerFromSorted(long key, ReadOnlySpan<long> sortedValues)
    {
        _buffersFreeListHeads.Allocate(ctx, BitmapContainerSizeInBytes, out ByteString storage);
        new Span<byte>(storage.Ptr, BitmapContainerSizeInBytes).Clear();

        OrSortedIntoBitmap((ulong*)storage.Ptr, sortedValues);

        AddNewContainer(key, ContainerType.Bitmap, new ContainerEntry
        {
            Cardinality = sortedValues.Length,
            Data = storage.Ptr,
            Storage = storage
        });
    }

    /// <summary>
    /// Idempotent, Caller tracks cardinality.
    /// </summary>
    private static void OrSortedIntoBitmap(ulong* bitmapPtr, ReadOnlySpan<long> sortedValues)
    {
        ushort firstLow = (ushort)(sortedValues[0] & ContainerValueMask);
        int currentWordIndex = firstLow >> 6;
        ulong currentMask = 1UL << (firstLow & 63);

        for (int j = 1; j < sortedValues.Length; j++)
        {
            ushort low = (ushort)(sortedValues[j] & ContainerValueMask);
            int wordIndex = low >> 6;

            if (wordIndex == currentWordIndex)
            {
                currentMask |= 1UL << (low & 63);
            }
            else
            {
                bitmapPtr[currentWordIndex] |= currentMask;
                currentWordIndex = wordIndex;
                currentMask = 1UL << (low & 63);
            }
        }
        bitmapPtr[currentWordIndex] |= currentMask;
    }


    private void CreateArrayContainerFromSorted(long key, ReadOnlySpan<long> sortedValues)
    {
        int neededBytes = sortedValues.Length * sizeof(ushort);
        _buffersFreeListHeads.Allocate(ctx, neededBytes, out ByteString storage);

        CopyDenseBottom16BitsToUshortArray(sortedValues, (ushort*)storage.Ptr);

        AddNewContainer(key, ContainerType.Array, new ContainerEntry
        {
            Cardinality = sortedValues.Length,
            Data = storage.Ptr,
            Storage = storage
        });
    }

    /// <summary>Batch-add sorted values into an existing container.</summary>
    private void AddRangeToContainer(int slot, ReadOnlySpan<long> sortedValues)
    {
        ref ContainerEntry entry = ref _entries[slot];
        ref ContainerType type = ref _types.RawItems[slot];

        switch (type)
        {
            case ContainerType.Bitmap:
            {
                // Lazy: bits are OR'd in; cardinality is marked dirty, so RepairAfterLazy or PrepareForReading recomputes it via popcount.
                OrSortedIntoBitmap((ulong*)entry.Data, sortedValues);
                entry.Cardinality = LazyCardinality;
                break;
            }

            case ContainerType.Range:
            {
                int rangeStart = entry.RangeStart;
                int rangeEnd = rangeStart + entry.Cardinality;

                // Check if all new values are contiguous and can extend one edge.
                ushort firstLow = (ushort)(sortedValues[0] & ContainerValueMask);
                ushort lastLow = (ushort)(sortedValues[^1] & ContainerValueMask);

                // Fully contained in existing range - noop.
                if (firstLow >= rangeStart && lastLow < rangeEnd)
                    break;

                bool contiguousBatch = lastLow - firstLow == sortedValues.Length - 1;
                if (contiguousBatch && TryMergeRangeInPlace(ref entry, firstLow, lastLow + 1))
                    break;

                // Non-contiguous batch (or disjoint contiguous batch) cannot stay as Range.
                if (MaybeConvertRangeToArray(ref entry, ref type, rangeStart, entry.Cardinality, sortedValues))
                    break;

                ConvertRangeToBitmap(ref entry, ref type);
                goto case ContainerType.Bitmap; // Convert to bitmap and add
            }

            case ContainerType.Array or ContainerType.ArrayUnsorted:
            {
                int newTotal = entry.Cardinality + sortedValues.Length;
                if (newTotal > ArrayContainerMaxCardinality)
                {
                    ConvertArrayToBitmap(ref entry, ref _types.RawItems[slot]);
                    goto case ContainerType.Bitmap; // Convert to bitmap and add
                }

                // Array is sorted & new data is directly following last value, can just append
                ushort firstNew = (ushort)(sortedValues[0] & ContainerValueMask);
                bool stillSorted = type == ContainerType.Array
                    && (entry.Cardinality == 0 || firstNew > entry.ArrayData[entry.Cardinality - 1]);

                int neededBytes = newTotal * sizeof(ushort);
                if (entry.Storage.Length < neededBytes)
                {
                    _buffersFreeListHeads.Allocate(ctx, neededBytes, out ByteString newStorage);
                    new Span<byte>(entry.Data, entry.Cardinality * sizeof(ushort))
                        .CopyTo(new Span<byte>(newStorage.Ptr, newStorage.Length));
                    _buffersFreeListHeads.Return(entry.Storage); // recycle old; new is larger
                    entry.Data = newStorage.Ptr;
                    entry.Storage = newStorage;
                }

                CopyDenseBottom16BitsToUshortArray(sortedValues, entry.ArrayData + entry.Cardinality);
                entry.Cardinality = newTotal;
                _types.RawItems[slot] = stillSorted ? ContainerType.Array : ContainerType.ArrayUnsorted;
                break;
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, "Unexpected container type in AddRange");
        }
    }

    /// <summary>
    /// Lazy OR skips per-container cardinality tracking: bitmap containers get Cardinality = -1 (dirty).
    /// Call RepairAfterLazy() once afterward to recompute cardinality in a single popcount pass.
    /// </summary>
    public void LazyOrWith(scoped ref RoaringBitmap other)
    {
        AssertNotConsumed();
        if (other.IsEmpty)
            return;

        int otherLen = other._index.Count;
        if (otherLen > 0)
            EnsureIndexCoversKey(otherLen - 1);

        _entries.EnsureCapacityFor(ctx, other.ContainerCount);
        _types.EnsureCapacityFor(ctx, other.ContainerCount);

        for (int key = 0; key < otherLen; key++)
        {
            int otherSlot = other.GetSlotForKey(key);
            if (otherSlot < 0)
                continue;

            ref ContainerEntry otherEntry = ref other._entries[otherSlot];
            int mySlot = GetSlotForKey(key);

            if (mySlot >= 0)
            {
                LazyOrContainerInPlace(ref _entries[mySlot], ref _types.RawItems[mySlot],
                    ref otherEntry, other._types.RawItems[otherSlot]);
                continue;
            }

            // Steal container from other (zero-copy).
            ContainerType otherType = other._types.RawItems[otherSlot];
            ContainerEntry stolen = otherEntry;
            otherEntry = default; // Clear the entry
            // Detach the stolen slot from the other
            other._types.RawItems[otherSlot] = ContainerType.Free;
            other._index.RawItems[key] = IndexAbsent;
            other._containerCount--;
            AddNewContainer(key, otherType, stolen);
        }

        MarkConsumed(ref other); // debug only - so we won't try to use it
    }

    public void OrWith(ref RoaringBitmap other)
    {
        AssertNotConsumed();
        LazyOrWith(ref other);
        RepairAfterLazy();
    }

    /// <summary>Lazy OR for a single container pair. Skips popcount — marks bitmap containers with Cardinality = -1.</summary>
    [SkipLocalsInit]
    private void LazyOrContainerInPlace(ref ContainerEntry left, ref ContainerType leftType, ref ContainerEntry right, ContainerType rightType)
    {
        switch (leftType, rightType)
        {
            case (ContainerType.Range, ContainerType.Range):
            {
                if (TryMergeRangeInPlace(ref left, right.RangeStart, RangeEndExclusive(ref right)))
                    return;

                if (TryConvertRangeRangeToBestContainer(ref left, ref leftType, ref right))
                    return;

                // Disjoint ranges: materialize the other range and keep lazy OR semantics.
                ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
                ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
                LazyOrContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
                break;
            }
            case (ContainerType.Range, _):
            {
                ConvertRangeToBitmap(ref left, ref leftType);
                LazyOrContainerInPlace(ref left, ref leftType, ref right, rightType);
                break;
            }
            case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Range):
            {
                ConvertArrayToBitmap(ref left, ref leftType); // allocate bitmap, will be "stoken" by the lazy or call 
                LazyOrContainerInPlace(ref left, ref leftType, ref right, rightType);
                break;
            }
            case (ContainerType.Bitmap, ContainerType.Range):
            {
                ulong* stackBitmap = stackalloc ulong[BitmapContainerSizeInUInt64]; // safe to use stack buffer, bitmap | bitmap won't steal the right-hand buffer
                ContainerEntry temp2 = MaterializeRangeIntoBuffer(ref right, stackBitmap);
                LazyOrContainerInPlace(ref left, ref leftType, ref temp2, ContainerType.Bitmap);
                break;
            }
            // From here, we no longer have ranges to worry about
            case (ContainerType.Bitmap, ContainerType.Bitmap):
            {
                // OR bitmaps without popcount — just bitwise OR
                BitmapOrNoPop(left.BitmapPtr, right.BitmapPtr, left.BitmapPtr);
                left.Cardinality = LazyCardinality; // mark dirty
                break;
            }
            case (ContainerType.Bitmap, ContainerType.Array or ContainerType.ArrayUnsorted):
            {
                // Set bits unconditionally — no per-bit cardinality check
                SetArrayInBitmap(right.ArrayData, right.Cardinality, left.BitmapPtr);
                left.Cardinality = LazyCardinality;
                break;
            }
            case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Array or ContainerType.ArrayUnsorted):
            {
                int maxResult = left.Cardinality + right.Cardinality;
                if (maxResult > ArrayContainerMaxCardinality)
                {
                    ConvertArrayToBitmap(ref left, ref leftType);
                    LazyOrContainerInPlace(ref left, ref leftType, ref right, rightType);
                    return;
                }
                // Append values left & right — duplicates are harmless, deduped on PrepareForReading.
                ConcatArrayContainers(ref left, ref right, maxResult);
                leftType = ContainerType.ArrayUnsorted;
                break;
            }
            case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Bitmap):
            {
                // Steal right's 8KB buffer
                Debug.Assert(right.Storage.Length is BitmapContainerSizeInBytes, "Right container bitmap buffer must be exactly 8KB");
                // OR left's array values into right's bitmap, then take ownership of the buffer.
                SetArrayInBitmap(left.ArrayData, left.Cardinality, right.BitmapPtr);
                if (left.Storage.HasValue)
                    _buffersFreeListHeads.Return(left.Storage);
                left.Storage = right.Storage;
                left.Data = right.Data;
                left.Cardinality = LazyCardinality;
                leftType = ContainerType.Bitmap;
                right = default;
                break;
            }
            default: // should never reach here
                throw new InvalidOperationException($"Unexpected container type pair: {leftType}, {rightType}");
        }
    }

    /// <summary>Recompute cardinality for all containers with Cardinality == -1). Allow to reduce popcount costs by batching them</summary>
    public void RepairAfterLazy()
    {
        for (int i = 0; i < _entries.Count; i++)
        {
            if (_types.RawItems[i] != ContainerType.Bitmap || // _types is a dense array, so cheaper to scan through it first
                _entries[i].Cardinality != LazyCardinality)
                continue;

            ref ContainerEntry entry = ref _entries[i];
             entry.Cardinality  = BitmapContainerCardinality(entry.Data);
            if (entry.Cardinality is 0)
                FreeContainer(entry.Key, i);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(long value)
    {
        AssertNotConsumed();
        if (value < 0)
            ThrowNegativeNotSupported(value);
        long key = value >> ContainerKeyShift;
        ushort low = (ushort)(value & ContainerValueMask);

        int slot = GetSlotForKey(key);
        if (slot >= 0)
        {
            ref ContainerEntry entry = ref _entries[slot];
            AddToContainer(ref entry, slot, low);
        }
        else
        {
            AddNewContainer(key, ContainerType.Range, new ContainerEntry
            {
                Cardinality = 1,
                RangeStart = low,
                Storage = default
            });
        }
    }

    public int Fill(Span<long> buffer, ref RoaringBitmapIterator iterator)
    {
        AssertNotConsumed();
        return iterator.Fill(ref this, buffer);
    }

    public RoaringBitmapIterator GetIterator()
    {
        AssertNotConsumed();
        return new RoaringBitmapIterator(ref this, ctx);
    }

    /// <summary>
    /// Filter a buffer in-place, keeping only values present in this bitmap. Buffer order is preserved; callers may pass entry IDs in any order.
    /// </summary>
    public int AndWith(Span<long> buffer, int count)
    {
        if (count == 0) return 0;

        int* idx = _index.RawItems;
        int idxLen = _index.Count;
        ContainerEntry* entries = _entries.RawItems;
        ContainerType* types = _types.RawItems;

        int kept = 0;
        for (int i = 0; i < count; i++)
        {
            long value = buffer[i];
            long containerKey = value >> ContainerKeyShift;
            if (containerKey < 0 || containerKey >= idxLen)
                continue;

            int slot = idx[containerKey];
            if (slot < 0) // absent or freed (FreeContainer resets _index[key] to IndexAbsent)
                continue;

            ushort low = (ushort)(value & ContainerValueMask);
            ref ContainerEntry entry = ref entries[slot];
            ref ContainerType type = ref types[slot];
            switch (type)
            {
                case ContainerType.Bitmap:
                    if (BitmapContains((ulong*)entry.Data, low) == false) continue;
                    break;

                case ContainerType.Array:
                    if (ArrayContainerContains(entry.ArrayData, entry.Cardinality, low) == false) continue;
                    break;

                case ContainerType.Range:
                    if (low < entry.RangeStart || low >= entry.RangeStart + entry.Cardinality) continue;
                    break;

                case ContainerType.ArrayUnsorted:
                    SortAndDedupSmallArray(ref entry, out type);
                    if (ArrayContainerContains(entry.ArrayData, entry.Cardinality, low) == false) continue;
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, "Unexpected container type in AndWith");
            }

            buffer[kept++] = value;
        }

        return kept;
    }

    /// <summary>Batch scoring helper: for each entry in matches that is present in this bitmap, add boostFactor to scores at that index.</summary>
    public void ScorePresentSorted(Span<long> sortedMatches, Span<float> scores, float boostFactor)
    {
        AssertNotConsumed();
        if (boostFactor == 0f)
            return;
        var visitor = new ScoreVisitor(scores, boostFactor);
        VisitPresentSorted(sortedMatches, sortedMatches.Length, ref visitor);
    }

    /// <summary>Count how many of <paramref name="sortedMatches"/> (ascending) are present in this bitmap, via a
    /// single grouped forward-cursor merge per container — O(matches + containers) instead of a point lookup per
    /// element.</summary>
    public int CountPresentSorted(Span<long> sortedMatches)
    {
        AssertNotConsumed();
        var visitor = new CountVisitor();
        VisitPresentSorted(sortedMatches, sortedMatches.Length, ref visitor);
        return visitor.Count;
    }

    /// <summary>Retain only the entries of <paramref name="sortedMatches"/> (ascending) that are present in this
    /// bitmap, compacting them to the front of the span in place; returns the retained count. One grouped
    /// forward-cursor merge per container, vs a point lookup per element. The inverse of the filtering that
    /// <see cref="DedupAddNew"/>'s visitor does (it keeps the absent; this keeps the present).</summary>
    public int RetainPresentSorted(Span<long> sortedMatches)
    {
        AssertNotConsumed();
        var visitor = new RetainVisitor(sortedMatches);
        VisitPresentSorted(sortedMatches, sortedMatches.Length, ref visitor);
        return visitor.Kept;
    }

    private interface IPresenceVisitor
    {
        bool VisitsMisses { get; }
        void OnHit(int index);
        void OnMiss(int index);

        void OnAbsentRun(int from, int to);
    }

    private struct CountVisitor : IPresenceVisitor
    {
        public int Count;
        public readonly bool VisitsMisses => false;
        public void OnHit(int index) => Count++;
        public readonly void OnMiss(int index) { }
        public readonly void OnAbsentRun(int from, int to) { }
    }

    // Keeps the present entries (the inverse of DedupVisitor): compacts each hit to the front in place. Hits are
    // visited in ascending index order and Kept <= index throughout, so the in-place write never clobbers an
    // unread element. VisitsMisses is false — absent entries (OnMiss / whole OnAbsentRun runs) are simply dropped.
    private ref struct RetainVisitor(Span<long> buffer) : IPresenceVisitor
    {
        private readonly Span<long> _buffer = buffer;
        public int Kept;

        public readonly bool VisitsMisses => false;
        public void OnHit(int index) => _buffer[Kept++] = _buffer[index];
        public readonly void OnMiss(int index) { }
        public readonly void OnAbsentRun(int from, int to) { }
    }

    private void VisitPresentSorted<TVisitor>(Span<long> sortedMatches, int count, scoped ref TVisitor visitor)
        where TVisitor : struct, IPresenceVisitor, allows ref struct
    {
        // Only the first `count` elements are the valid, sorted region. Callers such as DedupAddNew pass a
        // buffer whose tail (beyond count) is uninitialized ([SkipLocalsInit] stackalloc), so the assert must
        // be bounded to the region actually walked below — otherwise it trips on stack garbage in Debug.
        AssertSorted(sortedMatches[..count]);

        int* idx = _index.RawItems;
        int idxLen = _index.Count;
        ContainerEntry* entries = _entries.RawItems;
        ContainerType* types = _types.RawItems;
        bool visitsMisses = visitor.VisitsMisses;

        int i = 0;
        while (i < count)
        {
            long containerKey = sortedMatches[i] >> ContainerKeyShift;
            int groupEnd = Sorting.GallopLowerBound(sortedMatches, i, count, (containerKey + 1) << ContainerKeyShift);

            if (containerKey < 0 || containerKey >= idxLen || idx[containerKey] < 0)
            {
                visitor.OnAbsentRun(i, groupEnd);
                i = groupEnd;
                continue;
            }

            int slot = idx[containerKey];
            ref ContainerEntry entry = ref entries[slot];
            ref ContainerType type = ref types[slot];

            switch (type)
            {
                case ContainerType.Bitmap:
                {
                    ulong* bmp = (ulong*)entry.Data;
                    for (int gi = i; gi < groupEnd; gi++)
                    {
                        ushort low = (ushort)(sortedMatches[gi] & ContainerValueMask);
                        HandleVisitorHitMiss(ref visitor, BitmapContains(bmp, low), gi);
                    }
                    break;
                }

                case ContainerType.Range:
                {
                    int rangeStart = entry.RangeStart;
                    int rangeEnd = rangeStart + entry.Cardinality;
                    for (int gi = i; gi < groupEnd; gi++)
                    {
                        ushort low = (ushort)(sortedMatches[gi] & ContainerValueMask);
                        HandleVisitorHitMiss(ref visitor, low >= rangeStart && low < rangeEnd, gi);
                    }
                    break;
                }

                case ContainerType.Array:
                {
                    // Both sides ascending (values sorted, Array container sorted): one forward sweep.
                    ushort* arr = entry.ArrayData;
                    int arrLen = entry.Cardinality;
                    int ai = 0;
                    for (int gi = i; gi < groupEnd; gi++)
                    {
                        ushort low = (ushort)(sortedMatches[gi] & ContainerValueMask);
                        while (ai < arrLen && arr[ai] < low) ai++;
                        if (ai >= arrLen)
                        {
                            if (visitsMisses == false)
                                break; // Container exhausted: every remaining element in this run is a miss.
                            visitor.OnMiss(gi);
                            continue;
                        }
                        HandleVisitorHitMiss(ref visitor, arr[ai] == low, gi);
                    }
                    break;
                }
                case ContainerType.ArrayUnsorted:
                {
                    if (groupEnd - i == 1)
                    {
                        // Single probe: a SIMD linear scan is cheaper than sorting the whole container for it.
                        ushort low = (ushort)(sortedMatches[i] & ContainerValueMask);
                        HandleVisitorHitMiss(ref visitor, SimdLinearContains(entry.ArrayData, entry.Cardinality, low), i);
                    }
                    else
                    {
                        SortAndDedupSmallArray(ref entry, out type);
                        goto case ContainerType.Array;
                    }
                    break;
                }

                case ContainerType.Free: // Tombstone — nothing present here.
                    visitor.OnAbsentRun(i, groupEnd);
                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, "Unexpected container type in VisitPresentSorted");
            }

            i = groupEnd;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void HandleVisitorHitMiss(scoped ref TVisitor visitor, bool isHit, int index)
        {
            if (isHit) 
                visitor.OnHit(index);
            else 
                visitor.OnMiss(index);
        }
    }

    /// <summary>Adds <see cref="_boost"/> to the score slot of every present entry; ignores misses.</summary>
    private readonly ref struct ScoreVisitor(Span<float> scores, float boost) : IPresenceVisitor
    {
        private readonly Span<float> _scores = scores;
        private readonly float _boost = boost;

        public bool VisitsMisses => false;
        public void OnHit(int index) => _scores[index] += _boost;
        public void OnMiss(int index) { }
        public void OnAbsentRun(int from, int to) { }
    }

    /// <summary>KEEPS entries that are ABSENT from the bitmap  (compacting <see cref="_buffer"/> + <see cref="_indices"/> in place) and discards present ones.</summary>
    private ref struct DedupVisitor(Span<long> buffer, Span<int> indices) : IPresenceVisitor
    {
        private readonly Span<long> _buffer = buffer;
        private readonly Span<int> _indices = indices;
        public int Kept;

        public bool VisitsMisses => true;
        public void OnHit(int index) { } // present — discard

        public void OnMiss(int index)
        {
            _buffer[Kept] = _buffer[index];
            _indices[Kept] = _indices[index];
            Kept++;
        }

        public void OnAbsentRun(int from, int to)
        {
            for (int gi = from; gi < to; gi++)
            {
                _buffer[Kept] = _buffer[gi];
                _indices[Kept] = _indices[gi];
                Kept++;
            }
        }
    }

    /// <summary>Dedup + add in a single pass: for each entry in <paramref name="buffer"/>, if it is NOT already in the bitmap, keep it in the buffer and add it to the bitmap.
    /// Returns the count of new (non-duplicate) entries. The kept entries are restored to their original (input) order.</summary>
    [SkipLocalsInit]
    public int DedupAddNew(Span<long> buffer, int count)
    {
        if (count == 0) return 0;

        if(count <= 4096)
        {
            Span<int> temp = stackalloc int[4096];
            return DedupAddNew(buffer, count, temp);
        }
        
        using var _ = ctx.Allocate(PadToVector256Width(count), out Span<int> indices);
        return DedupAddNew(buffer, count, indices);
    }
    

    private int DedupAddNew(Span<long> buffer, int count, Span<int> indices)
    {
        if (count == 0) return 0;

        InitializeIndices(indices, count);
        buffer[..count].Sort(indices[..count]);
        count = RemoveDuplicates(buffer, indices, count);

        var visitor = new DedupVisitor(buffer, indices);
        VisitPresentSorted(buffer, count, ref visitor);
        int kept = visitor.Kept;

        // Add the new entries to the bitmap while they're still sorted.
        AddRange(buffer[..kept]);

        indices[..kept].Sort(buffer[..kept]); // restore original order
        return kept;
    }

    private static int RemoveDuplicates(Span<long> buffer, Span<int> indices, int count)
    {
        int unique = 1;
        for (int d = 1; d < count; d++)
        {
            if (buffer[d] == buffer[d - 1]) 
                continue;
            
            buffer[unique] = buffer[d];
            indices[unique] = indices[d];
            unique++;
        }

        return unique;
    }

    /// <summary>
    /// Bulk Select: for each rank in <paramref name="ranks"/>, write the corresponding set-bit value into <paramref name="results"/> at the same index.
    /// Out-of-range or negative ranks produce -1. <paramref name="ranks"/> is mutated (sorted in place).
    /// <paramref name="ranks"/> and <paramref name="results"/> must NOT alias - so aliasing would corrupt input.
    /// </summary>
    public void Select(ByteStringContext allocator, Span<long> ranks, Span<long> results)
    {
        int n = ranks.Length;
        Debug.Assert(results.Length >= n);
        Debug.Assert(ranks.Overlaps(results) == false, "ranks and results must not alias");
        if (n == 0)
            return;

        int paddedLen = PadToVector256Width(n);
        using var _ = allocator.Allocate(paddedLen * sizeof(int), out Span<int> indexes);
        InitializeIndices(indexes, n);

        ranks.Sort(indexes.Slice(0, n)); // indexes is padded to a Vector256 width for the SIMD fill; Sort requires equal-length spans.

        int rankIdx = 0;

        while (rankIdx < n && ranks[rankIdx] < 0)
        {
            results[indexes[rankIdx]] = -1;
            rankIdx++;
        }

        // Walk containers in key order, accumulating cardinality. Each rank lands inside the first container whose accumulated cardinality exceeds it.
        int* idx = _index.RawItems;
        int idxLen = _index.Count;
        ContainerEntry* entries = _entries.RawItems;
        ContainerType* types = _types.RawItems;

        long accCard = 0;
        for (int key = 0; key < idxLen && rankIdx < n; key++)
        {
            int slot = idx[key];
            if (slot < 0)
                continue;

            ref ContainerEntry entry = ref entries[slot];
            ref ContainerType type = ref types[slot];
            if (type == ContainerType.Free)
                continue;

            int card = ResolveCardinality(ref entry);
            if (type == ContainerType.ArrayUnsorted)
            {
                SortAndDedupSmallArray(ref entry, out type);
                card = entry.Cardinality; // dedup may have shrunk the container
            }

            long containerEnd = accCard + card;
            long baseValue = (long)key << ContainerKeyShift;
            int selWord = 0; // allows skipping repeated work when selecting multiple words on the same bitmap container
            int selPopBefore = 0;
            while (rankIdx < n && ranks[rankIdx] < containerEnd)
            {
                int localRank = (int)(ranks[rankIdx] - accCard);
                results[indexes[rankIdx]] = baseValue + SelectInContainer(ref entry, type, localRank, ref selWord, ref selPopBefore);
                rankIdx++;
            }
            accCard = containerEnd;
        }
        
        while (rankIdx < n) // Anything still unprocessed is past the end of the bitmap.
        {
            results[indexes[rankIdx]] = -1;
            rankIdx++;
        }
    }

    /// <summary>Round <paramref name="n"/> up to the next multiple of 8 (the AVX2 Vector256&lt;int&gt; lane width). Use this whenever allocating an int buffer that will be passed to
    /// <see cref="InitializeIndices"/> so the SIMD loop can store full 256-bit chunks without a scalar tail.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int PadToVector256Width(int n) => (n + 7) & ~7;

    /// <summary>Fill <paramref name="indices"/> with 0..read-1 using AVX2 256-bit stores (requires padding to allow proper SIMD write past indices span).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void InitializeIndices(Span<int> indices, int read)
    {
        Debug.Assert(PadToVector256Width(read) <= indices.Length, "SIMD write past indices span");
        ref int ptr = ref MemoryMarshal.GetReference(indices);

        var countVec = Vector256.Create(0, 1, 2, 3, 4, 5, 6, 7);
        var increment = Vector256.Create(8);

        int j = 0;
        while (j < read)
        {
            countVec.StoreUnsafe(ref Unsafe.Add(ref ptr, j));
            countVec += increment;
            j += 8;
        }
    }

    /// <summary>Find the nth set value inside a single container (0-based). Caller is responsible for upgrading ArrayUnsorted to Array first.</summary>
    private static int SelectInContainer(ref ContainerEntry entry, ContainerType type, int rank, ref int word, ref int popBefore)
    {
        switch (type)
        {
            case ContainerType.Array:
                return entry.ArrayData[rank];

            case ContainerType.Range:
                return entry.RangeStart + rank;

            case ContainerType.Bitmap:
            {
                ulong* bmp = (ulong*)entry.Data;
                int remaining = rank - popBefore; // index of the target among the set bits in bmp[word..)

                // scan 8 ulongs at a time. popcnt is a 1-cycle instruction with four-way ILP on modern x86, so an unrolled batch of 8 retires in ~3 cycles.
                // The JIT may also fuse this into AVX-512 VPOPCNTQ when supported.
                const int unroll = 8;
                while (word + unroll <= BitmapContainerSizeInUInt64)
                {
                    int blockBits =   BitOperations.PopCount(bmp[word])
                                    + BitOperations.PopCount(bmp[word + 1])
                                    + BitOperations.PopCount(bmp[word + 2])
                                    + BitOperations.PopCount(bmp[word + 3])
                                    + BitOperations.PopCount(bmp[word + 4])
                                    + BitOperations.PopCount(bmp[word + 5])
                                    + BitOperations.PopCount(bmp[word + 6])
                                    + BitOperations.PopCount(bmp[word + 7]);
                    if (remaining < blockBits)
                        break;
                    remaining -= blockBits;
                    popBefore += blockBits;
                    word += unroll;
                }

                for (; word < BitmapContainerSizeInUInt64; word++)
                {
                    ulong w = bmp[word];
                    int bits = BitOperations.PopCount(w);
                    if (remaining < bits)
                    {
                        // BMI2 PDEP deposits a single 1-bit at the remaining-th set position in w; trailing-zeros gives its bit index. One instruction.
                        // Note: word/popBefore are left at this word (not advanced) so the next, larger rank resumes here.
                        if (Bmi2.X64.IsSupported)
                        {
                            ulong target = Bmi2.X64.ParallelBitDeposit(1UL << remaining, w);
                            return word * 64 + BitOperations.TrailingZeroCount(target);
                        }

                        // Fallback: clear the lowest set bit `remaining` times.
                        while (remaining > 0)
                        {
                            w &= w - 1;
                            remaining--;
                        }
                        return word * 64 + BitOperations.TrailingZeroCount(w);
                    }
                    remaining -= bits;
                    popBefore += bits;
                }
                return -1; // shouldn't reach here if rank was valid
            }

            case ContainerType.ArrayUnsorted:
            case ContainerType.Free:
            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, "Unexpected container type in SelectInContainer");
        }
    }

    /// <summary>
    /// Create a deep copy of this bitmap. All container data is cloned into the same ByteStringContext.
    /// The source bitmap is not modified. The clone preserves container types (Range, Array, Bitmap).
    /// </summary>
    public RoaringBitmap Clone()
    {
        RoaringBitmap copy = new(ctx);

        // Walk entries directly using the Key field - no index indirection needed
        ContainerEntry* entries = _entries.RawItems;
        ContainerType* types = _types.RawItems;
        int entryCount = _entries.Count;
        for (int i = 0; i < entryCount; i++)
        {
            if (types[i] is ContainerType.Free) continue;
            ContainerEntry entry = CloneContainer(ctx, ref entries[i], types[i]);
            if (entry.Cardinality != 0) 
                copy.AddNewContainer(entries[i].Key, types[i], entry);
        }
        return copy;
    }

    // Threshold: below this count, comparison sort on a small array is cheaper than the bitmap radix sort path.
    private const int BitmapSortThreshold = 128;

    /// <summary>
    /// Prepare for reading: sort and deduplicate all unsorted array containers, and repair lazy bitmap cardinalities.
    /// </summary>
    [SkipLocalsInit]
    public void PrepareForReading()
    {
        AssertNotConsumed();
        ulong* scratch = stackalloc ulong[BitmapContainerSizeInUInt64];

        ContainerEntry* entries = _entries.RawItems;
        ContainerType* types = _types.RawItems;
        int entryCount = _entries.Count;
        for (int i = 0; i < entryCount; i++)
        {
            switch (types[i])
            {
                case ContainerType.ArrayUnsorted:
                {
                    ref ContainerEntry entry = ref entries[i];
                    if (entry.Cardinality >= BitmapSortThreshold)
                        SortViaBitmapScratch(ref entry, ref types[i], scratch);
                    else
                        SortAndDedupSmallArray(ref entry, out types[i]);
                    break;
                }

                case ContainerType.Bitmap when entries[i].Cardinality == LazyCardinality:
                {
                    ref ContainerEntry entry = ref entries[i];
                    entry.Cardinality = BitmapContainerCardinality(entry.Data);
                    if (entry.Cardinality == 0)
                        FreeContainer(entry.Key, i);
                    break;
                }
                case ContainerType.Array:
                case ContainerType.Range:
                case ContainerType.Bitmap:
                case ContainerType.Free:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(ContainerType), types[i], "Unexpected container type in PrepareForReading");
            }
        }
    }

    /// <summary>
    /// Radix sort using 8KB bitmap scratch space. O(n) bit-sets and O(n) word scan. Dedup is free (a duplicate bit-set is noop).
    /// IMPORTANT: we don't need to clear the whole bitmap, only the touched chunks.
    /// dirtyMap tracks which 4-ulong chunks have been written so extraction only visits touched chunks, skipping clean regions entirely.
    /// </summary>
    private static void SortViaBitmapScratch(ref ContainerEntry entry, ref ContainerType type, ulong* scratch)
    {
        Debug.Assert(type == ContainerType.ArrayUnsorted);

        var arr = entry.ArrayData;
        int count = entry.Cardinality;

        const int dirtyMapUlongLen = 4;
        ulong* dirtyMap = stackalloc ulong[dirtyMapUlongLen];
        Debug.Assert(dirtyMap[0] is 0 ,"This ensure that the stackalloc was cleared, *this* method should not have [SkipLocalsInit]");

        // Explode: set bits for each value, mark the chunk dirty on first touch and clear it.
        for (int i = 0; i < count; i++)
        {
            ushort val = arr[i];
            int wordIdx = val >> 6;
            int chunkIdx = wordIdx >> 2;
            if (BitmapContains(dirtyMap, (ushort)chunkIdx) == false)
            {
                BitmapSet(dirtyMap, (ushort)chunkIdx);
                new Span<byte>(scratch + chunkIdx * 4, 4 * sizeof(ulong)).Clear();
            }

            BitmapSet(scratch, val);
        }

        // Extract: only visit chunks marked dirty
        int sorted = 0;
        for (int i = 0; i < dirtyMapUlongLen; i++)
        {
            ulong currentWork = dirtyMap[i];
            while (currentWork != 0)
            {
                int chunkBit = BitOperations.TrailingZeroCount(currentWork);
                int wordBaseIdx = ((i << 6) + chunkBit) << 2;
                const int bitmapWordsPerBit = 4;
                for (int wordOffset = 0; wordOffset < bitmapWordsPerBit; wordOffset++)
                {
                    int wordIdx = wordBaseIdx + wordOffset;
                    ulong currentWord = scratch[wordIdx];
                    while (currentWord != 0)
                    {
                        int bit = BitOperations.TrailingZeroCount(currentWord);
                        arr[sorted++] = (ushort)((wordIdx << 6) + bit);
                        currentWord &= currentWord - 1;
                    }
                    scratch[wordIdx] = 0;
                }
                currentWork &= currentWork - 1;
            }
        }
        entry.Cardinality = sorted;
        type = ContainerType.Array;
    }

    /// <summary>
    /// In-place AND: retain only values that also exist in others. Walks both index arrays; containers in this bitmap with no match in other are freed.
    /// </summary>
    public void AndWith(scoped ref RoaringBitmap other)
    {
        AssertNotConsumed();
        int* myIdx = _index.RawItems;
        int myLen = _index.Count;

        for (int key = 0; key < myLen; key++)
        {
            int mySlot = myIdx[key];
            if (mySlot < 0)
                continue;

            int otherSlot = other.GetSlotForKey(key);
            if (otherSlot < 0)
            {
                // Not in other - remove; donate storage to other's free list so remaining container conversions on that side can reuse it
                FreeContainer(key, mySlot);
            }
            else
            {
                ref ContainerEntry myEntry = ref _entries[mySlot];
                ref ContainerEntry otherEntry = ref other._entries[otherSlot];
                AndContainerInPlace(ref myEntry, ref _types.RawItems[mySlot], ref otherEntry, other._types.RawItems[otherSlot]);
                int card = ResolveCardinality(ref myEntry);
                if (card == 0)
                    FreeContainer(key, mySlot);
            }
        }
        MarkConsumed(ref other);
    }

    /// <summary>
    /// Streaming AND for the limit-aware posting-list path: intersects in place only this bitmap's containers with key in [<paramref name="fromKeyInclusive"/>, <paramref name="toKeyExclusive"/>).
    /// Unlike <see cref="AndWith"/> it does NOT consume <paramref name="other"/>:  the caller grows <paramref name="other"/> across batches and can call AND on the same bitmap multiple times.
    /// Each time, we'll only work on the containers in the specified range.
    /// </summary>
    public long AndWithRange(scoped ref RoaringBitmap other, int fromKeyInclusive, int toKeyExclusive)
    {
        AssertNotConsumed();
        int* myIdx = _index.RawItems;
        int from = Math.Max(0, fromKeyInclusive);
        int to = Math.Min(_index.Count, toKeyExclusive);
        long survivors = 0;

        for (int key = from; key < to; key++)
        {
            int mySlot = myIdx[key];
            if (mySlot < 0)
                continue;

            int otherSlot = other.GetSlotForKey(key);
            if (otherSlot < 0)
            {
                // Settled container has no term entries — it cannot survive the intersection.
                FreeContainer(key, mySlot);
            }
            else
            {
                ref ContainerEntry myEntry = ref _entries[mySlot];
                ref ContainerEntry otherEntry = ref other._entries[otherSlot];
                AndContainerInPlace(ref myEntry, ref _types.RawItems[mySlot], ref otherEntry, other._types.RawItems[otherSlot]);
                int card = ResolveCardinality(ref myEntry);
                if (card == 0)
                    FreeContainer(key, mySlot);
                else
                    survivors += card;
            }
        }
        return survivors;
    }

    /// <summary>Drop every container whose key is ≥ <paramref name="fromKeyInclusive"/>. Used by the limit-aware streaming AND to discard the still-unintersected tail once enough survivors were found.</summary>
    public void RemoveContainersFrom(int fromKeyInclusive)
    {
        AssertNotConsumed();
        int* myIdx = _index.RawItems;
        int myLen = _index.Count;
        for (int key = Math.Max(0, fromKeyInclusive); key < myLen; key++)
        {
            int mySlot = myIdx[key];
            if (mySlot >= 0)
                FreeContainer(key, mySlot);
        }
    }

    /// <summary>
    /// In-place ANDNOT: remove all values that exist in other from this bitmap.
    /// </summary>
    public void AndNotWith(scoped ref RoaringBitmap other)
    {
        AssertNotConsumed();
        int myLen = _index.Count;
        int* myIdx = _index.RawItems;

        for (int key = 0; key < myLen; key++)
        {
            int mySlot = myIdx[key];
            if (mySlot < 0)
                continue;

            int otherSlot = other.GetSlotForKey(key);
            if (otherSlot < 0)
                continue; // nothing to subtract

            ref ContainerEntry otherEntry = ref other._entries[otherSlot];
            AndNotContainerInPlace(ref _entries[mySlot], ref _types.RawItems[mySlot], ref otherEntry, other._types.RawItems[otherSlot]);
            if (_entries[mySlot].Cardinality == 0)
                FreeContainer(key, mySlot);
        }
        MarkConsumed(ref other);
    }

    [SkipLocalsInit]
    private void AndContainerInPlace(ref ContainerEntry left, ref ContainerType leftType, ref ContainerEntry right, ContainerType rightType)
    {
        switch (leftType, rightType)
        {
            // Range×Range fast paths - no allocation needed
            case (ContainerType.Range, ContainerType.Range):
            {
                int intersectStart = Math.Max(left.RangeStart, right.RangeStart);
                int intersectEnd = Math.Min(RangeEndExclusive(ref left), RangeEndExclusive(ref right));
                if (intersectStart >= intersectEnd)
                {
                    left.Cardinality = 0;
                }
                else
                {
                    left.RangeStart = (ushort)intersectStart;
                    left.Cardinality = intersectEnd - intersectStart;
                }
                return;
            }
            case (ContainerType.Range, _):
            {
                ConvertRangeToBitmap(ref left, ref leftType);
                AndContainerInPlace(ref left, ref leftType, ref right, rightType);
                return;
            }
            case (_, ContainerType.Range):
            {
                ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
                ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
                AndContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
                return;
            }
            case (ContainerType.Bitmap, ContainerType.Bitmap):
            {
                // Lazy: bitwise AND only; PrepareForReading will popcount + free if empty.
                BitmapAndNoPop(left.BitmapPtr, right.BitmapPtr, left.BitmapPtr);
                left.Cardinality = LazyCardinality;
                break;
            }
            case (ContainerType.Bitmap, ContainerType.Array or ContainerType.ArrayUnsorted):
            {
                // AND bitmap with an array: build the intersection in a stack scratch by
                // OR'ing only values that are set in left; then copy back. Lazy cardinality.
                ushort* arr = right.ArrayData;
                var bmp = left.BitmapPtr;
                ulong* scratch = stackalloc ulong[BitmapContainerSizeInUInt64];
                new Span<byte>(scratch, BitmapContainerSizeInBytes).Clear();

                for (int i = 0; i < right.Cardinality; i++)
                {
                    ushort val = arr[i];
                    if (BitmapContains(bmp, val))
                        BitmapSet(scratch, val);
                }

                new Span<byte>(scratch, BitmapContainerSizeInBytes)
                    .CopyTo(new Span<byte>(left.Data, BitmapContainerSizeInBytes));
                left.Cardinality = LazyCardinality;
                break;
            }
            case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Bitmap):
            {
                // Filter left's values against right's bitmap, in-place. Order doesn't matter.
                ushort* arr = left.ArrayData;
                var bmp = right.BitmapPtr;
                int count = 0;
                for (int i = 0; i < left.Cardinality; i++)
                {
                    ushort val = arr[i];
                    if (BitmapContains(bmp, val))
                        arr[count++] = val;
                }
                left.Cardinality = count;
                break;
            }
            case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Array or ContainerType.ArrayUnsorted):
            {
                if (AdvInstructionSet.IsAcceleratedVector256
                    && left.Cardinality <= SimdLinearScanThreshold
                    && right.Cardinality <= SimdLinearScanThreshold)
                {
                    Debug.Assert(left.Storage.Length % Vector256<ushort>.Count == 0 && right.Storage.Length % Vector256<ushort>.Count == 0,
                            "array containers must be SIMD-aligned for SIMD AND, see SimdContains for details");

                    left.Cardinality = SimdCrossAnd(left.ArrayData, left.Cardinality, right.ArrayData, right.Cardinality, left.ArrayData);
                }
                else
                {
                    // Need sorted arrays for galloping merge
                    if (leftType == ContainerType.ArrayUnsorted)
                        SortAndDedupSmallArray(ref left, out leftType);
                    if (rightType == ContainerType.ArrayUnsorted)
                        SortAndDedupSmallArray(ref right, out rightType);
                    ushort* a = left.ArrayData;
                    ushort* b = right.ArrayData;
                    left.Cardinality = ArrayContainerAnd(a, left.Cardinality, b, right.Cardinality, a);
                }
                break;
            }
            default:
                throw new InvalidOperationException($"Invalid container combination: {leftType} and {rightType}");
        }
    }

    [SkipLocalsInit]
    private void AndNotContainerInPlace(ref ContainerEntry left, ref ContainerType leftType, ref ContainerEntry right, ContainerType rightType)
    {
        switch (leftType, rightType)
        {
            case (ContainerType.Range, ContainerType.Range):
            {
                AndNotTwoRangesInPlace(ref left, ref leftType, ref right);
                break;
            }
            case (ContainerType.Range, _):
            {
                ConvertRangeToBitmap(ref left, ref leftType);
                AndNotContainerInPlace(ref left, ref leftType, ref right, rightType);
                break;
            }
            case (_, ContainerType.Range):
            {
                ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
                ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
                AndNotContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
                break;
            }
            case (ContainerType.Bitmap, ContainerType.Bitmap):
            {
                // Lazy: bitwise ANDNOT only.
                BitmapAndNotNoPop(left.BitmapPtr, right.BitmapPtr, left.BitmapPtr);
                left.Cardinality = LazyCardinality;
                break;
            }
            case (ContainerType.Bitmap, ContainerType.Array or ContainerType.ArrayUnsorted):
            {
                // Lazy: clear bits unconditionally — no per-bit cardinality check.
                ClearArrayInBitmap(right.ArrayData, right.Cardinality, left.BitmapPtr);
                left.Cardinality = LazyCardinality;
                break;
            }
            case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Bitmap):
            {
                // Keep left values NOT in the right's bitmap. Order doesn't matter.
                ushort* arr = left.ArrayData;
                var bmp = right.BitmapPtr;
                int count = 0;
                for (int i = 0; i < left.Cardinality; i++)
                {
                    ushort val = arr[i];
                    if (BitmapContains(bmp, val) == false)
                        arr[count++] = val;
                }

                left.Cardinality = count;
                break;
            }
            case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Array or ContainerType.ArrayUnsorted):
            {
                // merge small arrays using SIMD
                if (AdvInstructionSet.IsAcceleratedVector256
                    && left.Cardinality <= SimdLinearScanThreshold
                    && right.Cardinality <= SimdLinearScanThreshold)
                {
                    Debug.Assert(left.Storage.Length % Vector256<ushort>.Count == 0 && right.Storage.Length % Vector256<ushort>.Count == 0,
                        "array containers must be SIMD-aligned for SIMD ANDNOT, see SimdCrossAndNot for details");
                    left.Cardinality = SimdCrossAndNot(left.ArrayData, left.Cardinality, right.ArrayData, right.Cardinality, left.ArrayData);
                    break;
                }

                if (leftType == ContainerType.ArrayUnsorted)
                    SortAndDedupSmallArray(ref left, out leftType);
                if (rightType == ContainerType.ArrayUnsorted)
                    SortAndDedupSmallArray(ref right, out rightType);
                ushort* a = left.ArrayData;
                ushort* b = right.ArrayData;
                left.Cardinality = ArrayContainerAndNot(a, left.Cardinality, b, right.Cardinality, a);
                break;
            }
            default:
                throw new InvalidOperationException($"Invalid container combination: {leftType} and {rightType}");
        }
    }

    private void AndNotTwoRangesInPlace(ref ContainerEntry left, ref ContainerType leftType, ref ContainerEntry right)
    {
        int leftStart = left.RangeStart;
        int leftEnd = RangeEndExclusive(ref left);
        int rightStart = right.RangeStart;
        int rightEnd = RangeEndExclusive(ref right);

        // No overlap.
        if (rightEnd <= leftStart || rightStart >= leftEnd)
            return;

        // Right is covering all of left.
        if (rightStart <= leftStart && rightEnd >= leftEnd)
        {
            left.Cardinality = 0;
            return;
        }

        // Trim low edge.
        if (rightStart <= leftStart)
        {
            left.RangeStart = (ushort)rightEnd;
            left.Cardinality = leftEnd - rightEnd;
            return;
        }

        // Trim high edge.
        if (rightEnd >= leftEnd)
        {
            left.Cardinality = rightStart - leftStart;
            return;
        }

        // Middle cut would split into two ranges - materialize.
        ConvertRangeToBitmap(ref left, ref leftType);
        ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
        ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
        AndNotContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
    }

    /// <summary>
    /// Materialize a Range container into the given stackalloc bitmap buffer. The returned entry points at that buffer with Storage=default, so the caller must NOT release it.
    /// </summary>
    [SkipLocalsInit]
    private static ContainerEntry MaterializeRangeIntoBuffer(ref ContainerEntry entry, ulong* stackBitmap)
    {
        ClearBitmap(stackBitmap);
        FillBitmapFromRange(stackBitmap, entry.RangeStart, entry.Cardinality);

        return new ContainerEntry
        {
            Data = (byte*)stackBitmap,
            Cardinality = entry.Cardinality,
            Storage = default // no allocation to release
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureIndexCoversKey(long key)
    {
        if (key < _index.Count)
            return;

        IncreaseIndexCapacity(key);
    }

    private void IncreaseIndexCapacity(long key)
    {
        if (key is < 0 or >= int.MaxValue - 16)
            throw new ArgumentOutOfRangeException(nameof(key), $"Container key {key} is out of the valid range (0..{int.MaxValue - 17}).");

        int needed = checked((int)(key + 1));
        int oldCount = _index.Count;
        _index.EnsureCapacityFor(ctx, needed - oldCount);
        _index.Count = needed;

        // Fill new slots with -1 (absent)
        int* ptr = _index.RawItems;
        new Span<int>(ptr + oldCount, needed - oldCount).Fill(IndexAbsent);
    }

    private int AddNewContainer(long key, ContainerType type, in ContainerEntry entry)
    {
        EnsureIndexCoversKey(key);

        int slot;
        if (_containersFreeListHead != 0) // 0 = empty; non-zero = slot+1 (1-based)
        {
            slot = _containersFreeListHead - 1; // decode: real index = stored - 1
            Debug.Assert(_types.RawItems[slot] == ContainerType.Free, "Expected free entry");
            _containersFreeListHead = (int)_entries[slot].NextFreeSlot; // next encoded value (0 or slot+1)
            _entries[slot] = entry;
            _types.RawItems[slot] = type;
        }
        else
        {
            slot = _entries.Count;
            _entries.Add(ctx, entry);
            _types.Add(ctx, type);
        }

        _entries[slot].Key = (uint)key; // we checked size of key in EnsureIndexCoversKey, so this cast is safe

        _index.RawItems[key] = slot;
        _containerCount++;
        return slot;
    }

    private void FreeContainer(long key, int slot)
    {
        ref ContainerEntry entry = ref _entries[slot];
        if (entry.Storage.HasValue)
            _buffersFreeListHeads.Return(entry.Storage);

        entry = default;
        _types.RawItems[slot] = ContainerType.Free;
        entry.NextFreeSlot = (uint)_containersFreeListHead; // current head (0 or prev_slot+1)
        _containersFreeListHead = slot + 1;                 // encode: store as real_index + 1

        _index.RawItems[key] = IndexAbsent;
        _containerCount--;
    }

    private void ConcatArrayContainers(ref ContainerEntry left, ref ContainerEntry right, int totalCount)
    {
        int neededBytes = totalCount * sizeof(ushort);
        Debug.Assert(neededBytes <= BitmapContainerSizeInBytes, "Total count exceeds maximum for array container");
        int leftCardinality = left.Cardinality;
        if (neededBytes > right.Storage.Length)
        {
            EnsureArrayCapacity(ref left, totalCount);
            Unsafe.CopyBlockUnaligned(left.ArrayData + leftCardinality, right.ArrayData, (uint)(right.Cardinality * sizeof(ushort)));
            left.Cardinality = totalCount;
            return;
        }
        // Right buffer fits both — append left's values after right, then take ownership (order is irrelevant for ArrayUnsorted).
        Unsafe.CopyBlockUnaligned(right.ArrayData + right.Cardinality, left.ArrayData, (uint)(leftCardinality * sizeof(ushort)));
        if (left.Storage.HasValue)
            ctx.Release(ref left.Storage);
        left.Storage = right.Storage;
        left.Data = right.Data;
        left.Cardinality = totalCount;
        right = default;
    }

    /// <summary>
    /// Ensure the array container has room for the given number of entries.
    /// Doubles the buffer size up to BitmapContainerSizeInBytes (8KB).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureArrayCapacity(ref ContainerEntry entry, int requiredEntries)
    {
        int requiredBytes = requiredEntries * sizeof(ushort);
        if (requiredBytes <= entry.Storage.Length)
            return;

        IncreaseArrayCapacity(ref entry, requiredBytes);
    }

    private void IncreaseArrayCapacity(ref ContainerEntry entry, int requiredBytes)
    {
        int newSize = Math.Max(entry.Storage.Length * 2, requiredBytes);
        newSize = Math.Min(newSize, BitmapContainerSizeInBytes);

        _buffersFreeListHeads.Allocate(ctx, newSize, out ByteString newStorage);
        int copyBytes = entry.Cardinality * sizeof(ushort);
        if (copyBytes > 0)
            Unsafe.CopyBlockUnaligned(newStorage.Ptr, entry.Data, (uint)copyBytes);

        if (entry.Storage.HasValue)
            _buffersFreeListHeads.Return(entry.Storage);
        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
    }


    [SkipLocalsInit]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddToContainer(ref ContainerEntry entry, int slot, ushort value)
    {
        ref ContainerType type = ref _types.RawItems[slot];
        switch (type)
        {
            case ContainerType.Array:
                // If value is >= the last element, append (or noop for duplicate) and stay sorted
                if (entry.Cardinality > 0 && value >= entry.ArrayData[entry.Cardinality - 1])
                {
                    if (value == entry.ArrayData[entry.Cardinality - 1])
                        break; // duplicate of last element - noop
                    if (entry.Cardinality >= ArrayContainerMaxCardinality)
                    {
                        ConvertArrayToBitmap(ref entry, ref type);
                        BitmapSet(entry.BitmapPtr, value);
                        entry.Cardinality = LazyCardinality;
                        break;
                    }
                    EnsureArrayCapacity(ref entry, entry.Cardinality + 1);
                    entry.ArrayData[entry.Cardinality++] = value;
                    break;
                }
                // Would break sort order - switch to unsorted for O(1) appends
                type = ContainerType.ArrayUnsorted;
                goto case ContainerType.ArrayUnsorted;

            case ContainerType.ArrayUnsorted:
                if (entry.Cardinality >= ArrayContainerMaxCardinality)
                {
                    // At capacity: sort+dedup via bitmap scratch, then promote if still full
                    ulong* scratch = stackalloc ulong[BitmapContainerSizeInUInt64];
                    SortViaBitmapScratch(ref entry, ref type, scratch); // type → Array, deduped
                    if (entry.Cardinality >= ArrayContainerMaxCardinality)
                    {
                        ConvertArrayToBitmap(ref entry, ref type);
                        goto case ContainerType.Bitmap;
                    }
                    goto case ContainerType.Array;// we are now sorted (and not full), we'll let the array handler deal with it.
                }
                EnsureArrayCapacity(ref entry, entry.Cardinality + 1);
                entry.ArrayData[entry.Cardinality++] = value;
                break;

            case ContainerType.Bitmap:
                BitmapSet(entry.BitmapPtr, value);
                entry.Cardinality = LazyCardinality;
                break;

            case ContainerType.Range:
                if (TryMergeRangeInPlace(ref entry, value, value + 1) == false)
                {
                    ConvertRangeForAdd(ref entry, ref type, value);
                }
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type), $"Unexpected value to add: {type}");
        }
    }

    /// <summary>Convert an array container (sorted or unsorted) to bitmap. The resulting bitmap is marked dirty  (Cardinality = LazyCardinality)</summary>
    private void ConvertArrayToBitmap(ref ContainerEntry entry, ref ContainerType type)
    {
        Debug.Assert(type is ContainerType.Array or ContainerType.ArrayUnsorted);

        ushort* arr = entry.ArrayData;
        int count = entry.Cardinality;

        _buffersFreeListHeads.Allocate(ctx, BitmapContainerSizeInBytes, out ByteString newStorage);
        ClearBitmap((ulong*)newStorage.Ptr);
        SetArrayInBitmap(arr, count, (ulong*)newStorage.Ptr);

        if (entry.Storage.HasValue)
            _buffersFreeListHeads.Return(entry.Storage);

        entry.Cardinality = LazyCardinality;
        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        type = ContainerType.Bitmap;
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;

        if (_entries.IsValid)
        {
            ContainerEntry* entries = _entries.RawItems;
            int count = _entries.Count;
            for (int i = 0; i < count; i++)
            {
                if (entries[i].Storage.HasValue)
                    ctx.Release(ref entries[i].Storage);
            }

            _entries.Dispose(ctx);
        }


        _buffersFreeListHeads.ReleaseAll(ctx);

        if (_types.IsValid)
            _types.Dispose(ctx);

        if (_index.IsValid)
            _index.Dispose(ctx);
    }
}
