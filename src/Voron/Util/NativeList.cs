using System;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;

namespace Voron.Util;

public unsafe struct NativeList<T> : IDisposable
    where T: unmanaged
{
    private readonly ByteStringContext _ctx;

    public int Count;

    public int Capacity;

    public T* RawItems;

    private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseItems;

    public bool IsValid => _ctx != null;

    public NativeList(ByteStringContext ctx)
    {
        _ctx = ctx;
    }

    public void Add(T l)
    {
        if (Count == Capacity)
        {
            GrowListUnlikely(1);
        }

        RawItems[Count++] = l;
    }
    
    private void GrowListUnlikely(int addition)
    {
        Capacity = Math.Max(16, Bits.PowerOf2(Capacity + addition));
        var scope = _ctx.Allocate(Capacity * sizeof(T), out var mem);
        if (RawItems != null)
        {
            Memory.Copy(mem.Ptr, RawItems, Count * sizeof(T));
            _releaseItems.Dispose();
        }
        _releaseItems = scope;
        RawItems = (T*)mem.Ptr;
    }
    
    public void Dispose()
    {
        _releaseItems.Dispose();
    }

    public readonly void Sort()
    {
        new Span<T>(RawItems, Count).Sort();
    }
}
