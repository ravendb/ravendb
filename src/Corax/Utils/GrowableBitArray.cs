using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;
using Sparrow.Server;

namespace Corax.Utils;

internal unsafe struct GrowableBitArray : IDisposable
{
    internal const long DefaultMaxSetBit = -1L;
    internal static readonly int MaxCapacityPerBitmap = (int.MaxValue - sizeof(ByteStringStorage)) / sizeof(ulong);
    internal static readonly long MaxCapacityPerBitmapInBits = MaxCapacityPerBitmap * 64L;
    private BitArray[] _bitArrays;
    private readonly long _capacity;

    public long MinSetBit { get; private set; }

    public long MaxSetBit { get; private set; }

    /// <summary>
    /// Creates a new bit array. It accepts when bits id are between [0, capacity]
    /// </summary>
    public GrowableBitArray(ByteStringContext allocator, long capacity)
    {
        MinSetBit = long.MaxValue;
        MaxSetBit = DefaultMaxSetBit;
        _capacity = capacity + 1; // ensure it's not zero and handles the last bit inclusively.
        var numberOfUlongsToAllocate = _capacity / 64 + (_capacity % 64 == 0 ? 0 : 1);
        var numberOfBitArrays = (int)Math.Ceiling(numberOfUlongsToAllocate / (double)MaxCapacityPerBitmap);
        _bitArrays = new BitArray[numberOfBitArrays];
        var lastChunkSize = (int)(numberOfUlongsToAllocate - (long)(numberOfBitArrays - 1) * MaxCapacityPerBitmap);
        for (int i = 0; i < numberOfBitArrays; ++i)
        {
            _bitArrays[i] = new BitArray(allocator, i == numberOfBitArrays - 1
                ? lastChunkSize
                : MaxCapacityPerBitmap);
        }
    }

    public int FillAnd(in GrowableBitArray other, Span<long> matches, long from)
    {
        Debug.Assert(_capacity == other._capacity);

        from = Math.Max(from, Math.Max(MinSetBit, other.MinSetBit));
        var maxBit = Math.Min(MaxSetBit, other.MaxSetBit);
        if (from > maxBit)
            return 0;

        int total = 0;
        var chunkIdx = (int)(from / MaxCapacityPerBitmapInBits);
        var lastChunkIdx = (int)(maxBit / MaxCapacityPerBitmapInBits);
        var inChunkFrom = from % MaxCapacityPerBitmapInBits;

        for (; chunkIdx <= lastChunkIdx && total < matches.Length; chunkIdx++)
        {
            var toWordExclusive = chunkIdx == lastChunkIdx
                ? (int)(maxBit % MaxCapacityPerBitmapInBits / 64) + 1
                : int.MaxValue;
            total += _bitArrays[chunkIdx].FillAnd(other._bitArrays[chunkIdx], matches.Slice(total), inChunkFrom, chunkIdx * MaxCapacityPerBitmapInBits, toWordExclusive);
            inChunkFrom = 0;
        }

        return total;
    }

    public int Fill(Span<long> matches, long from)
    {
        from = Math.Max(from, MinSetBit);
        var maxBit = MaxSetBit;
        if (from > maxBit)
            return 0;

        int total = 0;
        var chunkIdx = (int)(from / MaxCapacityPerBitmapInBits);
        var lastChunkIdx = (int)(maxBit / MaxCapacityPerBitmapInBits);
        var inChunkFrom = from % MaxCapacityPerBitmapInBits;

        for (; chunkIdx <= lastChunkIdx && total < matches.Length; chunkIdx++)
        {
            var toWordExclusive = chunkIdx == lastChunkIdx
                ? (int)(maxBit % MaxCapacityPerBitmapInBits / 64) + 1
                : int.MaxValue;
            total += _bitArrays[chunkIdx].Fill(matches.Slice(total), inChunkFrom, chunkIdx * MaxCapacityPerBitmapInBits, toWordExclusive);
            inChunkFrom = 0;
        }

        return total;
    }

#if DEBUG
    public bool IsValid
    {
        get
        {
            for (int i = 0; i < _bitArrays.Length; ++i)
                if (_bitArrays[i].IsValid == false)
                    return false;
            return true;
        }
    }
#endif

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Add(long pos)
    {
        if (pos >= _capacity)
            throw new ArgumentOutOfRangeException($"Tried to modify the bit at position '{pos}', however the capacity is only {_capacity}");
        var bitmapIdx = (int)(pos / MaxCapacityPerBitmapInBits);
        var added = _bitArrays[(int)bitmapIdx].Add(pos - bitmapIdx * MaxCapacityPerBitmapInBits);
        if (added)
        {
            if (pos < MinSetBit)
                MinSetBit = pos;
            if (pos > MaxSetBit)
                MaxSetBit = pos;
        }

        return added;
    }

    public void AddRange(Span<long> ids)
    {
        if (ids.Length == 0)
            return;

        // Fill batches are sorted within themselves, so the window is first/last of the batch.
        Debug.Assert(ids[0] <= ids[ids.Length - 1], "AddRange expects a batch sorted in ascending order");
        if (ids[0] < MinSetBit)
            MinSetBit = ids[0];
        if (ids[ids.Length - 1] > MaxSetBit)
            MaxSetBit = ids[ids.Length - 1];

        if (_bitArrays.Length == 1)
        {
            _bitArrays[0].AddRange(ids);
            return;
        }

        foreach (var id in ids)
            Add(id);
    }

    public int Subtract(Span<long> ids)
    {
        var min = MinSetBit;
        var max = MaxSetBit;
        if (max < 0)
            return ids.Length; // empty bitmap - everything is kept

        int kept = 0;
        for (int i = 0; i < ids.Length; i++)
        {
            var id = ids[i];
            ids[kept] = id;
            if (id < min || id > max || Contains(id) == false)
                kept++;
        }

        return kept;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool Contains(long pos)
    {
        var bitmapIdx = (int)(pos / MaxCapacityPerBitmapInBits);
        return _bitArrays[bitmapIdx].Contains(pos - bitmapIdx * MaxCapacityPerBitmapInBits);
    }

    public void Dispose()
    {
        if (_bitArrays == null)
            return;

        for (int i = 0; i < _bitArrays.Length; ++i)
            _bitArrays[i].Dispose();
        _bitArrays = null;
    }

    private struct BitArray : IDisposable
    {
        private ulong* _bits;
        private IDisposable _memoryScope;
        private int _length;
#if DEBUG
        public bool IsValid = true;
#endif
        public BitArray(ByteStringContext allocator, int numberOfUlongsToAllocate)
        {
            _length = numberOfUlongsToAllocate;
            _memoryScope = allocator.Allocate(numberOfUlongsToAllocate * sizeof(ulong), out ByteString memory);
            memory.ToSpan<ulong>().Clear();
            _bits = (ulong*)memory.Ptr;
#if DEBUG
            IsValid = true;
#endif
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Add(long id)
        {
            var mask = 1UL << (int)(id & 63);
            var bucket = _bits + (int)(id >> 6);
            var result = *bucket & mask;
            *bucket |= mask;
            return result == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(long id)
        {
            return (*(_bits + (int)(id >> 6)) & (1UL << (int)(id & 63))) != 0;
        }

        public void AddRange(Span<long> ids)
        {
            foreach (var id in ids)
            {
                var mask = 1UL << (int)(id & 63);
                var bucket = _bits + (int)(id >> 6);
                *bucket |= mask;
            }
        }

        public int FillAnd(BitArray otherBitArray, Span<long> matches, long from, long shift, int toWordExclusive)
        {
            toWordExclusive = Math.Min(toWordExclusive, _length);
            int count = 0;
            var it = (int)(from / 64);
            if (it >= toWordExclusive)
                return 0;

            ref long matchesRef = ref MemoryMarshal.GetReference(matches);
            var word = *(_bits + it) & *(otherBitArray._bits + it) & (ulong.MaxValue << (int)(from % 64));

            while (true)
            {
                if (word != 0)
                {
                    var wordBase = shift + (long)it * 64;
                    while (word != 0 && count < matches.Length)
                    {
                        Unsafe.Add(ref matchesRef, count++) = wordBase + BitOperations.TrailingZeroCount(word);
                        word &= word - 1;
                    }

                    if (count == matches.Length)
                        return count;
                }
                else if (AdvInstructionSet.IsAcceleratedVector128)
                {
                    while (it + 1 + Vector512<ulong>.Count <= toWordExclusive &&
                           (Vector512.Load(_bits + it + 1) & Vector512.Load(otherBitArray._bits + it + 1)) == Vector512<ulong>.Zero)
                    {
                        it += Vector512<ulong>.Count;
                    }
                }

                it++;
                if (it >= toWordExclusive)
                    return count;
                word = *(_bits + it) & *(otherBitArray._bits + it);
            }
        }

        // Single-bitmap twin of FillAnd - keep the two loops in sync.
        public int Fill(Span<long> matches, long from, long shift, int toWordExclusive)
        {
            toWordExclusive = Math.Min(toWordExclusive, _length);
            int count = 0;
            var it = (int)(from / 64);
            if (it >= toWordExclusive)
                return 0;

            ref long matchesRef = ref MemoryMarshal.GetReference(matches);
            var word = *(_bits + it) & (ulong.MaxValue << (int)(from % 64));

            while (true)
            {
                if (word != 0)
                {
                    var wordBase = shift + (long)it * 64;
                    while (word != 0 && count < matches.Length)
                    {
                        Unsafe.Add(ref matchesRef, count++) = wordBase + BitOperations.TrailingZeroCount(word);
                        word &= word - 1;
                    }

                    if (count == matches.Length)
                        return count;
                }
                else if (AdvInstructionSet.IsAcceleratedVector128)
                {
                    while (it + 1 + Vector512<ulong>.Count <= toWordExclusive &&
                           Vector512.Load(_bits + it + 1) == Vector512<ulong>.Zero)
                    {
                        it += Vector512<ulong>.Count;
                    }
                }

                it++;
                if (it >= toWordExclusive)
                    return count;
                word = *(_bits + it);
            }
        }

        public void Dispose()
        {
#if DEBUG
            IsValid = false;
#endif
            _memoryScope?.Dispose();
            _bits = null;
            _memoryScope = null;
        }
    }
}
