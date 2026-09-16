using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow.Server.Utils.VxSort;

namespace Sparrow.Server.Collections;

public unsafe struct GrowableBitArray : IDisposable
{
    internal const long DefaultMaxSetBit = -1L;
    internal static readonly int MaxCapacityPerBitmap = (int.MaxValue - sizeof(ByteStringStorage)) / sizeof(ulong);
    internal static readonly long MaxCapacityPerBitmapInBits = MaxCapacityPerBitmap * 64L;
    private BitArray[] _bitArrays;
    public readonly long Capacity;

    /// <summary>
    /// The owner must update this count manually.
    /// </summary>
    public long Count;

    /// <summary>
    /// Inclusive window containing all set bits. <see cref="Remove"/> does not shrink it, so it may be wider than the actual set bits.
    /// </summary>
    public long MinSetBit { get; private set; }

    public long MaxSetBit { get; private set; }

    /// <summary>
    /// Creates a new bit array. It accepts when bits id is between [0, capacity]
    /// </summary>
    public GrowableBitArray(ByteStringContext allocator, long capacity)
    {
        MinSetBit = long.MaxValue;
        MaxSetBit = DefaultMaxSetBit;
        Capacity = capacity + 1; // ensure it's not zero and handles the last bit inclusively.
        var numberOfUlongsToAllocate = Capacity / 64 + (Capacity % 64 == 0 ? 0 : 1);
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

    public Iterator GetIterator(long from) => new Iterator(this, from);

    public ref struct Iterator : IEnumerator<long>
    {
        private readonly GrowableBitArray _bitArray;
        private long _from;
        private int _currentBitArrayIdx;
        private long _currentShift;
        private BitArray.Iterator _iterator;

        public Iterator(GrowableBitArray bitArray, long from)
        {
            _bitArray = bitArray;
            _from = from;
            Reset();
        }

        public bool MoveNext()
        {
            while (_currentBitArrayIdx < _bitArray._bitArrays.Length)
            {
                if (_iterator.MoveNext())
                {
                    return true;
                }

                _currentShift += MaxCapacityPerBitmapInBits;
                _currentBitArrayIdx++;

                if (_currentBitArrayIdx < _bitArray._bitArrays.Length)
                    _iterator = new(_bitArray._bitArrays[_currentBitArrayIdx], 0);
            }

            return false;
        }

        public void Reset() => Reset(_from);

        public void Reset(long from)
        {
            _from = from;

            _currentBitArrayIdx = (int)(_from / MaxCapacityPerBitmapInBits);
            _currentShift = _currentBitArrayIdx * MaxCapacityPerBitmapInBits;

            if (_currentBitArrayIdx <= _bitArray._bitArrays.Length)
                _iterator = new(_bitArray._bitArrays[_currentBitArrayIdx], (int)(_from % MaxCapacityPerBitmapInBits));
        }

        public long Current => _currentShift + _iterator.Current;

        object IEnumerator.Current
        {
            get => Current;
        }

        public void Dispose()
        {
            //nothing to dispose
        }
    }

    public int FillAnd(in GrowableBitArray other, Span<long> matches, long from)
    {
        Debug.Assert(Capacity == other.Capacity);

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
        if (pos >= Capacity)
            throw new ArgumentOutOfRangeException($"Tried to modify the bit at position '{pos}', however the capacity is only {Capacity}");
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

    // Requirement: ids sorted
    public void AddRange(Span<long> ids)
    {
        AssertIsSorted(ids);

        if (ids.Length == 0)
            return;

        if (ids[0] < MinSetBit)
            MinSetBit = ids[0];
        if (ids[^1] > MaxSetBit)
            MaxSetBit = ids[^1];

        if (_bitArrays.Length == 1)
        {
            _bitArrays[0].AddRange(ids);
            return;
        }

        foreach (var id in ids)
            Add(id);
    }

    [Conditional("DEBUG")]
    private static void AssertIsSorted(Span<long> entries)
    {
        var count = entries.Length;
        if (count <= 1)
        {
            // If there are 0 or 1 elements, it is considered sorted
            return;
        }

        for (int i = 0; i < count - 1; i++)
        {
            Debug.Assert(entries[i] >= 0);
            if (entries[i] > entries[i + 1])
            {
                throw new InvalidOperationException("The entries are not sorted.");
            }
        }
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
    public void Remove(long pos)
    {
        if (pos >= Capacity)
            throw new ArgumentOutOfRangeException($"Tried to modify the bit at position '{pos}', however the capacity is only {Capacity}");
        var bitmapIdx = (int)(pos / MaxCapacityPerBitmapInBits);
        _bitArrays[(int)bitmapIdx].Remove(pos - bitmapIdx * MaxCapacityPerBitmapInBits);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(long pos)
    {
        var bitmapIdx = (int)(pos / MaxCapacityPerBitmapInBits);
        return _bitArrays[(int)bitmapIdx].Contains(pos - bitmapIdx * MaxCapacityPerBitmapInBits);
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
        public void Remove(long id)
        {
            var mask = ~(1UL << (int)(id & 63));
            var bucket = _bits + (int)(id >> 6);
            *bucket &= mask;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(long id)
        {
            var mask = 1UL << (int)(id & 63);
            var bucket = _bits + (int)(id >> 6);
            return (*bucket & mask) != 0;
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

        //adjusted from src/Raven.Server/Documents/Queries/LuceneIntegration/FastBitArray.cs:7
        public ref struct Iterator : IEnumerator<long>
        {
            private int _it;
            private ulong _bitmap = 0;
            private int _count = 0;
            private readonly BitArray _array;
            private readonly int _from;

            public Iterator(BitArray array, int from)
            {
                _from = from;
                _array = array;
                Reset();
            }

            public bool MoveNext()
            {
                if (_it >= _array._length)
                    return false;

                while (true)
                {
                    if (_bitmap != 0)
                    {
                        ulong t = _bitmap & (ulong)-(long)_bitmap;
                        _count = BitOperations.TrailingZeroCount(_bitmap);
                        _bitmap ^= t;
                        return true;
                    }

                    _it++;
                    if (_it >= _array._length)
                        break;

                    _bitmap = *(_array._bits + _it);
                }

                return false;
            }

            public void Reset()
            {
                _it = _from / 64;
                _bitmap = 0;
                _count = 0;

                _bitmap = *(_array._bits + _it);
                _bitmap &= ulong.MaxValue << (_from % 64);
            }

            public long Current => _it * 64 + _count;

            object IEnumerator.Current
            {
                get => Current;
            }

            public void Dispose()
            {
                // nothing to dispose
            }
        }

        public unsafe IEnumerable<int> Iterate(int from)
        {
            // https://lemire.me/blog/2018/02/21/iterating-over-set-bits-quickly/
            int i = from / 64;
            if (i >= _length)
                yield break;

            ulong bitmap;
            unsafe
            {
                bitmap = *(_bits + i);
                bitmap &= ulong.MaxValue << (from % 64);
            }

            while (true)
            {
                while (bitmap != 0)
                {
                    ulong t = bitmap & (ulong)-(long)bitmap;
                    int count = BitOperations.TrailingZeroCount(bitmap);
                    int setBitPos = i * 64 + count;
                    yield return setBitPos;
                    bitmap ^= t;
                }

                i++;
                if (i >= _length)
                    break;
                unsafe
                {
                    bitmap = *(_bits + i);
                }
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
