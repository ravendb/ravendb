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
    private int _capacity;
    public long* RawItems;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseItems;

    public NativeIntegersList(ByteStringContext ctx, int initialCapacity = 0)
    {
        _ctx = ctx;
        RawItems = null;
        _releaseItems = default;
        Count = 0;
        _capacity = 0;
        GrowListUnlikely(initialCapacity);
    }

    public void Add(ReadOnlySpan<long> values)
    {
        if (Count + values.Length >= _capacity)
        {
            GrowListUnlikely(values.Length);
            Debug.Assert(Count + values.Length <= _capacity);
        }

        values.CopyTo(new Span<long>(RawItems + Count, _capacity - Count));
        Count += values.Length;
    }
    public void Add(long l)
    {
        if (Count == _capacity)
        {
            GrowListUnlikely(1);
        }

        RawItems[Count++] = l;
    }

    public Span<long> Items =>  new(RawItems, Count);

    private void GrowListUnlikely(int addition)
    {
        _capacity = Math.Max(16, Bits.PowerOf2(_capacity + addition));
        var scope = _ctx.Allocate(_capacity * sizeof(long), out var mem);
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

    public int MoveTo(Span<long> matches)
    {
        var read = Math.Min(Count, matches.Length);
        new Span<long>(this.RawItems, read).CopyTo(matches);
        Count -= read;
        RawItems += read;
        return read;
    }

    public void Clear()
    {
        Count = 0;
    }
}
