using System;
using System.Diagnostics;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;

namespace Voron.Data.Sets;

public unsafe struct NativeIntegersList : IDisposable
{
    private readonly ByteStringContext _ctx;
    public int Count;
    public int Capacity;
    private long* _items;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseItems;

    public NativeIntegersList(ByteStringContext ctx)
    {
        _ctx = ctx;
        _items = null;
        _releaseItems = default;
        Count = 0;
        Capacity = 0;
    }

    public void Add(ReadOnlySpan<long> values)
    {
        if (Count + values.Length >= Capacity)
        {
            GrowListUnlikely(values.Length);
            Debug.Assert(Count + values.Length <= Capacity);
        }

        values.CopyTo(new Span<long>(_items + Count, Capacity - Count));
        Count += values.Length;
    }
    public void Add(long l)
    {
        if (Count == Capacity)
        {
            GrowListUnlikely(1);
        }

        _items[Count++] = l;
    }

    public Span<long> Items =>  new(_items, Count);

    private void GrowListUnlikely(int addition)
    {
        Capacity = Math.Max(16, Bits.PowerOf2(Capacity + addition));
        var scope = _ctx.Allocate(Capacity * sizeof(long), out var mem);
        if (_items != null)
        {
            Memory.Copy(mem.Ptr, _items, Count * sizeof(long));
            _releaseItems.Dispose();
        }
        _releaseItems = scope;
        _items = (long*)mem.Ptr;
    }

    public void Dispose()
    {
        _releaseItems.Dispose();
    }
}
