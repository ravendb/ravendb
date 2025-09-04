using System;
using System.Runtime.CompilerServices;
using Sparrow.Server;

namespace Corax.Utils;

internal unsafe struct GrowableBitArray : IDisposable
{
    internal static readonly int MaxCapacityPerBitmap = (int.MaxValue - sizeof(ByteStringStorage)) / sizeof(ulong);
    internal static readonly long MaxCapacityPerBitmapInBits = MaxCapacityPerBitmap * 64L;
    private BitArray[] _bitArrays;
    private readonly long _capacity;
    
    /// <summary>
    /// Creates a new growable bit array. It accepts when bits id between [0, capacity]
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
    public bool Add(long id)
    {
        if (id > _capacity)
            throw new ArgumentOutOfRangeException($"Tried to modify bit no {id}, however the capacity is only { _capacity * 64}");
        var bitmapIdx = (int)(id / MaxCapacityPerBitmapInBits);
        return _bitArrays[(int)bitmapIdx].Add(id - bitmapIdx * MaxCapacityPerBitmapInBits);
    }

    public void Dispose()
    {
        for (int i = 0; i < _bitArrays.Length; ++i)
            _bitArrays[i].Dispose();
    }

    private unsafe struct BitArray : IDisposable
    {
        private ulong* _bits;
        private IDisposable _memoryScope;
#if DEBUG
        public bool IsValid = true;
#endif
        public BitArray(ByteStringContext allocator, int numberOfUlongsToAllocate)
        {
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

        public void Dispose()
        {
#if DEBUG
            IsValid = false;
#endif
            _memoryScope.Dispose();
        }
    }
}
