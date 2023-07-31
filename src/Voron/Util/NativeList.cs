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
    private int _count;

    private int _capacity;

    private T* _rawItems;

    public T* RawItems => _rawItems;
    public int Capacity => _capacity;
    public int Count => _count;

    public readonly Span<T> ToSpan() => new(_rawItems, _count);
    
    public bool IsValid => RawItems != null;

    public NativeList()
    {
        _rawItems = null;
    }

    public bool TryPush(in T l)
    {
        if (_count == _capacity)
            return false;

        _rawItems[_count++] = l;
        return true;
    }

    public void PushUnsafe(in T l)
    {
        Debug.Assert(Count < Capacity);
        _rawItems[_count++] = l;
    }

    public ref T AddByRefUnsafe()
    {
        Debug.Assert(Count < Capacity);
        return ref _rawItems[_count++];
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
        
        value = _rawItems[--_count];
        return true;
    }

    public T PopUnsafe()
    {
        return _rawItems[--_count];
    }

    public ByteStringContext<ByteStringMemoryCache>.InternalScope Initialize(ByteStringContext ctx, int count = 16)
    {
        _capacity = Math.Max(16, Bits.PowerOf2(count));

        var scope = ctx.Allocate(_capacity * sizeof(T), out var mem);
        _rawItems = (T*)mem.Ptr;
        return scope;
    }
    
    public void Grow(ByteStringContext ctx, int addition, ref ByteStringContext<ByteStringMemoryCache>.InternalScope currentScope)
    {
        _capacity = Math.Max(16, Bits.PowerOf2(_capacity + addition));
        var scope = ctx.Allocate(_capacity * sizeof(T), out var mem);
        if (RawItems != null)
        {
            Memory.Copy(mem.Ptr, _rawItems, _count * sizeof(T));
            currentScope.Dispose();
        }
        _rawItems = (T*)mem.Ptr;
        currentScope = scope;
    }

    public readonly void Sort()
    {
        if (typeof(T) == typeof(int))
        {
            Sorting.Run(new Span<int>(_rawItems, _count));
        }
        else if (typeof(T) == typeof(long))
        {
            Sorting.Run(new Span<long>(_rawItems, _count));
        }
        else
        {
            new Span<T>(_rawItems, _count).Sort();
        }
    }

    public bool HasCapacityFor(int itemsToAdd)
    {
        return _count + itemsToAdd < _capacity;
    }

    public void ResetAndEnsureCapacity(ByteStringContext ctx, int size, ref ByteStringContext<ByteStringMemoryCache>.InternalScope currentScope)
    {
        if (size > _capacity)
            Grow(ctx, size - _capacity + 1, ref currentScope);

        // We will reset.
        _count = 0;
    }
}
