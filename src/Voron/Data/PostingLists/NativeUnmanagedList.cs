using System;
using System.Diagnostics;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;

namespace Voron.Data.PostingLists;

//This is based on the NativeIntegerList. If you make any changes to it, please go to the base class and ensure that everything is correct.
public unsafe struct NativeUnmanagedList<T> : IDisposable
    where T : unmanaged
{
    private readonly ByteStringContext _ctx;
    public int Count { get; private set; }
    public int Capacity { get; private set; }
    public T* RawItems { get; private set; }
    public readonly bool Initialized = false; 
    
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseItems;
    public Span<T> Items =>  new(RawItems, Count);

    public NativeUnmanagedList(ByteStringContext ctx, int initialCapacity = 0)
    {
        _ctx = ctx;
        RawItems = null;
        _releaseItems = default;
        Count = 0;
        Capacity = 0;
        Initialized = true;
        if (initialCapacity != -1)
        {
            GrowListUnlikely(initialCapacity);
        }
    }
    
    public void Add(ReadOnlySpan<T> values)
    {
        if (Count + values.Length >= Capacity)
        {
            GrowListUnlikely(values.Length);
            Debug.Assert(Count + values.Length <= Capacity);
        }

        values.CopyTo(new Span<T>(RawItems + Count, Capacity - Count));
        Count += values.Length;
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

    public int MoveTo(Span<T> output)
    {
        if (Initialized == false || output.Length == 0)
            return 0;

        var count = Math.Min(Count, output.Length);
        new Span<T>(RawItems, count).CopyTo(output);

        // Check if we have moved the entire content.
        if (Count == count)
        {
            Count = 0;
            return count;
        }

        var theRestToCopy = new Span<T>(RawItems + count, Count - count);
        theRestToCopy.CopyTo(new Span<T>(RawItems, Capacity));
        Count = theRestToCopy.Length;

        return count;
    }

    public void Dispose()
    {
        _releaseItems.Dispose();
    }
}
