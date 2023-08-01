using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Sorting = Sparrow.Server.Utils.VxSort.Sort;

namespace Voron.Util;

public unsafe struct NativeList<T>
    where T: unmanaged
{
    private ByteString _storage;

    private int _count;

    public T* RawItems => (T*)_storage.Ptr;
    public int Capacity => _storage.Length / sizeof(T);
    public int Count => _count;

    public readonly Span<T> ToSpan() => new(_storage.Ptr, _count);
    
    public bool IsValid => RawItems != null;

    public NativeList()
    {
        _storage = default;
    }

    public bool TryPush(in T l)
    {
        if (_count == Capacity)
            return false;

        RawItems[_count++] = l;
        return true;
    }

    public void PushUnsafe(in T l)
    {
        Debug.Assert(Count < Capacity);
        RawItems[_count++] = l;
    }

    public ref T AddByRefUnsafe()
    {
        Debug.Assert(Count < Capacity);
        return ref RawItems[_count++];
    }


    public void Shrink(int newSize)
    {
        if (newSize > _count)
            throw new InvalidOperationException("The new size cannot be bigger than the current size.");

        _count = newSize;
    }

    public bool TryPop(out T value)
    {
        if (_count == 0)
        {
            Unsafe.SkipInit(out value);
            return false;
        }
        
        value = RawItems[--_count];
        return true;
    }

    public T PopUnsafe()
    {
        return RawItems[--_count];
    }

    public void Initialize(ByteStringContext ctx, int count = 16)
    {
        var capacity = Math.Max(16, Bits.PowerOf2(count));
        ctx.Allocate(capacity * sizeof(T), out _storage);
    }
    
    public void Grow(ByteStringContext ctx, int addition)
    {
        var capacity = Math.Max(16, Bits.PowerOf2(Capacity + addition));
        ctx.Allocate(capacity * sizeof(T), out var mem);

        if (_storage.HasValue)
        {
            Memory.Copy(mem.Ptr, _storage.Ptr, _count * sizeof(T));
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

    public bool HasCapacityFor(int itemsToAdd)
    {
        return _count + itemsToAdd < Capacity;
    }

    public void ResetAndEnsureCapacity(ByteStringContext ctx, int size)
    {
        if (size > Capacity)
            Grow(ctx, size - Capacity + 1);

        // We will reset.
        _count = 0;
    }

    public void Dispose(ByteStringContext ctx)
    {
        ctx.Release(ref _storage);
    }
}
