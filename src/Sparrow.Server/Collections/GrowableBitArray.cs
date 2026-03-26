using System;
using System.Collections;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow.Server.Utils.VxSort;

namespace Sparrow.Server.Collections;

public unsafe struct GrowableBitArray : IDisposable
{
    internal static readonly int MaxCapacityPerBitmap = (int.MaxValue - sizeof(ByteStringStorage)) / sizeof(ulong);
    internal static readonly long MaxCapacityPerBitmapInBits = MaxCapacityPerBitmap * 64L;
    private BitArray[] _bitArrays;
    public readonly long Capacity;

    /// <summary>
    /// The owner must update this count manually.
    /// </summary>
    public long Count;

    /// <summary>
    /// Creates a new bit array. It accepts when bits id is between [0, capacity]
    /// </summary>
    public GrowableBitArray(ByteStringContext allocator, long capacity)
    {
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
        return _bitArrays[(int)bitmapIdx].Add(pos - bitmapIdx * MaxCapacityPerBitmapInBits);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Remove(long pos)
    {
        if (pos >= Capacity)
            throw new ArgumentOutOfRangeException($"Tried to modify the bit at position '{pos}', however the capacity is only {Capacity}");
        var bitmapIdx = (int)(pos / MaxCapacityPerBitmapInBits);
        _bitArrays[(int)bitmapIdx].Remove(pos - bitmapIdx * MaxCapacityPerBitmapInBits);
    }
    
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
