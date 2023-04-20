using System;
using System.Diagnostics;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;

namespace Voron.Data.PostingLists;

public unsafe struct NativeIntegersList : IDisposable
{
    private readonly ByteStringContext _ctx;
    public int Count;
    public int Capacity;
    public long* RawItems;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseItems;

    public NativeIntegersList(ByteStringContext ctx)
    {
        _ctx = ctx;
        RawItems = null;
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

        values.CopyTo(new Span<long>(RawItems + Count, Capacity - Count));
        Count += values.Length;
    }
    public void Add(long l)
    {
        if (Count == Capacity)
        {
            GrowListUnlikely(1);
        }

        RawItems[Count++] = l;
    }

    public Span<long> Items =>  new(RawItems, Count);

    private void GrowListUnlikely(int addition)
    {
        Capacity = Math.Max(16, Bits.PowerOf2(Capacity + addition));
        var scope = _ctx.Allocate(Capacity * sizeof(long), out var mem);
        if (RawItems != null)
        {
            Memory.Copy(mem.Ptr, RawItems, Count * sizeof(long));
            _releaseItems.Dispose();
        }
        _releaseItems = scope;
        RawItems = (long*)mem.Ptr;
    }

    public void Dispose()
    {
        _releaseItems.Dispose();
    }

    public void Sort()
    {
        if (Count <= 1)
            return;
        
        Sparrow.Server.Utils.VxSort.Sort.Run(RawItems, Count);
    }
}
