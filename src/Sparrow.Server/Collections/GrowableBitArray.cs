using System;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow.Binary;

namespace Sparrow.Server.Collections;

public unsafe struct GrowableBitArray : IDisposable
{
    internal static readonly int MaxCapacityPerBitmap = (int.MaxValue - sizeof(ByteStringStorage)) / sizeof(ulong);
    internal static readonly long MaxCapacityPerBitmapInBits = MaxCapacityPerBitmap * 64L;
    private BitArray[] _bitArrays;
    private readonly long _capacity;
    
    /// <summary>
    /// The owner must update this count manually.
    /// </summary>
    public long Count;

    /// <summary>
    /// Creates a new bit array. It accepts when bits id is between [0, capacity]
    /// </summary>
    public GrowableBitArray(ByteStringContext allocator, long capacity)
    {
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

    public IEnumerable<long> Iterate(long from)
    {
        for (int bitArrayIdx = (int)(from / MaxCapacityPerBitmapInBits); bitArrayIdx < _bitArrays.Length; bitArrayIdx++)
        {
            var shift = bitArrayIdx * MaxCapacityPerBitmapInBits;
            foreach (var result in _bitArrays[bitArrayIdx].Iterate((int)(from % MaxCapacityPerBitmapInBits)))
            {
                yield return result + shift;
            }
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
        if (pos >= _capacity)
            throw new ArgumentOutOfRangeException($"Tried to modify the bit at position '{pos}', however the capacity is only {_capacity}");
        var bitmapIdx = (int)(pos / MaxCapacityPerBitmapInBits);
        return _bitArrays[(int)bitmapIdx].Add(pos - bitmapIdx * MaxCapacityPerBitmapInBits);
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
        public bool Contains(long id)
        {
            var mask = 1UL << (int)(id & 63);
            var bucket = _bits + (int)(id >> 6);
            return (*bucket & mask) != 0;
        }

        //adjusted from src/Raven.Server/Documents/Queries/LuceneIntegration/FastBitArray.cs:7
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
