using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Sorting = Sparrow.Server.Utils.VxSort.Sort;

namespace Voron.Util;

/// <summary>
/// NativeList of T is a very low level primitive where you have to deal with the context as an external entity (correctness is on the caller's hands).
/// ContextBoundNativeList of T is the high level primitive that should be used for most uses and purposes. For example, there are cases where the
/// NativeList has to contain other native lists, therefore, the requirement of NativeList of T to be completely unmanaged is important for those uses.
/// </summary>
public unsafe struct NativeList<T>()
    where T : unmanaged
{
    private ByteString _storage = default;

    public T* RawItems => Capacity > 0 ? (T*)_storage.Ptr : null;

    public int Capacity => _storage.Length / sizeof(T);
    public int Count;

    public readonly Span<T> ToSpan() => Count == 0 ? Span<T>.Empty : new Span<T>(_storage.Ptr, Count);
    
    public bool IsValid => RawItems != null;

    public ref T this[int index]
    {
        get => ref Unsafe.AsRef<T>((T*)_storage.Ptr + index);
    }

    public bool TryAdd(in T l)
    {
        if (Capacity == 0 || Count == Capacity)
            return false;

        RawItems[Count++] = l;
        return true;
    }

    public bool TryAddRange(ReadOnlySpan<T> values)
    {
        if (Count + values.Length >= Capacity)
            return false;

        values.CopyTo(new Span<T>(RawItems + Count, Capacity - Count));
        Count += values.Length;
        return true;
    }

    public void Add(ByteStringContext ctx, T value)
    {
        EnsureCapacityFor(ctx, 1);
        AddUnsafe(value);
    }

    public void InitializeWithValue(ByteStringContext allocator, T value, int count)
    {
        EnsureCapacityFor(allocator, count);
        if (Capacity == 0) 
            return;
        
        Count = count;
        ToSpan().Fill(value);
    }

    public void AddRangeUnsafe(ReadOnlySpan<T> range)
    {
        Debug.Assert(Count + range.Length <= Capacity);
        Debug.Assert((uint)(range.Length * sizeof(T)) > (uint)range.Length || range.Length == 0);

        Unsafe.CopyBlockUnaligned(
            ref Unsafe.AsRef<byte>(RawItems + Count), 
            ref MemoryMarshal.GetReference(MemoryMarshal.Cast<T, byte>(range)), 
            (uint)(range.Length * sizeof(T)));

        Count += range.Length;
    }
    public void AddRangeUnsafe(T* items, int count)
    {
        Debug.Assert(Count + count <= Capacity);
        Debug.Assert((uint)(count * sizeof(T)) > (uint)count || count == 0);

        Unsafe.CopyBlock(RawItems + Count, items, (uint)(count * sizeof(T)));
        Count += count;
    }

    public void AddUnsafe(in T l)
    {
        Debug.Assert(Count < Capacity);
        RawItems[Count++] = l;
    }

    public ref T AddByRefUnsafe()
    {
        Debug.Assert(Count < Capacity);
        return ref RawItems[Count++];
    }


    public void Shrink(int newSize)
    {
        if (newSize > Count)
            throw new InvalidOperationException("The new size cannot be bigger than the current size.");

        Count = newSize;
    }

    public void Initialize(ByteStringContext ctx, int count = 1)
    {
        var capacity = count == 1 ? 1 : Math.Max(1, Bits.PowerOf2(count));
        ctx.Allocate(capacity * sizeof(T), out _storage);
    }
    
    public void Grow(ByteStringContext ctx, int addition)
    {
        var capacity = Math.Max(1, Bits.PowerOf2(Capacity + addition));
        ctx.Allocate(capacity * sizeof(T), out var mem);

        if (_storage.HasValue)
        {
            Memory.Copy(mem.Ptr, _storage.Ptr, Count * sizeof(T));
            ctx.Release(ref _storage);
        }

        _storage = mem;
    }

    public readonly void Sort()
    {
        if (typeof(T) == typeof(int) || typeof(T) == typeof(long))
        {
            Sorting.Run(ToSpan());
        }
        else
        {
            ToSpan().Sort();
        }
    }

    public void EnsureCapacityFor(ByteStringContext allocator,int additionalItems)
    {
        if (HasCapacityFor(additionalItems))
            return;
        Grow(allocator, additionalItems);
    }

    public bool HasCapacityFor(int itemsToAdd)
    {
        return Count + itemsToAdd < Capacity;
    }

    public void ResetAndEnsureCapacity(ByteStringContext ctx, int size)
    {
        if (size > Capacity)
            Grow(ctx, size - Capacity + 1);

        // We will reset.
        Count = 0;
    }

    public int CopyTo(Span<T> destination, int startFrom) 
    {
        if (Count == 0)
            return 0;

        var count = Math.Min(Count - startFrom, destination.Length);
        new Span<T>(RawItems + startFrom, count).CopyTo(destination);

        return count;
    }

    public void CopyTo(Span<T> destination, int startFrom, int count)
    {
        if (Count == 0)
            return;

        if (Math.Min(Count - startFrom, destination.Length) < count)
            throw new ArgumentOutOfRangeException(nameof(count));

        new Span<T>(RawItems + startFrom, count).CopyTo(destination);
    }

    public void Dispose(ByteStringContext ctx)
    {
        if(_storage.HasValue)
            ctx.Release(ref _storage);
    }

    public void Clear()
    {
        Count = 0;
    }
    

    public Enumerator GetEnumerator() => new(RawItems, Count);

    public struct Enumerator : IEnumerator<T>
    {
        private readonly T* _items;
        private readonly int _len;
        private int _index;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Enumerator(T* items, int len)
        {
            _items = items;
            _len = len;
            _index = -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            int index = _index + 1;
            if (index < _len)
            {
                _index = index;
                return true;
            }

            return false;
        }

        public void Reset()
        {
            _index = -1;
        }

        object IEnumerator.Current => Current;

        public T Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _items[_index];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
        }
    }

#if CORAX_MEMORY_WATCHER
    public (long BytesUsed, long BytesAllocated) Allocations => (Count * sizeof(T), Capacity * sizeof(T));
#endif
}
