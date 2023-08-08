using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Server.Utils.VxSort;
using Voron.Util;

namespace Voron.Data.PostingLists;

// When making changes here, please remember to update NativeUnmanagedList if necessary.
public unsafe struct NativeIntegersList : IDisposable
{
    private readonly ByteStringContext _ctx;
    public int Count;
    public int Capacity;
    public long* RawItems;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _releaseItems;

    public NativeIntegersList(ByteStringContext ctx, int initialCapacity = 0)
    {
        _ctx = ctx;
        RawItems = null;
        _releaseItems = default;
        Count = 0;
        Capacity = 0;
        if (initialCapacity != -1)
        {
            GrowListUnlikely(initialCapacity);
        }
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
    
    public void Add(long* values, int count)
    {
        if (Count + count > Capacity)
        {
            GrowListUnlikely(count);
            Debug.Assert(Count + count <= Capacity);
        }

        AddUnsafe(values, count);
    }

    public void AddUnsafe(long* values, int count)
    {
        Debug.Assert(Count + count <= Capacity);
        Unsafe.CopyBlock(RawItems + Count, values, (uint)(count * sizeof(long)));
        Count += count;
    }

    public void Add(long l)
    {
        if (Count == Capacity)
        {
            GrowListUnlikely(1);
        }
        Debug.Assert(RawItems + Count < _releaseItems.GetStringEnd());
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

    public void SortAndRemoveDuplicates()
    {
        if (Count <= 1)
            return;
        
        Count = Sorting.SortAndRemoveDuplicates(RawItems, Count);
    }

    public void SortAndRemoveDuplicatesAndRemovals()
    {
        if (Count <= 1)
            return;
        Sort.Run(RawItems, Count);
        RemoveDuplicatedAndRemovals();
    }
    private void RemoveDuplicatedAndRemovals()
    {
        var outputBufferPtr = RawItems;

        var bufferPtr = outputBufferPtr;
        var bufferEndPtr = bufferPtr + Count - 1;
        Debug.Assert((*bufferPtr & 1) == 0);
        while (bufferPtr < bufferEndPtr)
        {
            outputBufferPtr += ((bufferPtr[1] & ~1) != bufferPtr[0]).ToInt32();
            *outputBufferPtr = bufferPtr[1];
            outputBufferPtr -= (bufferPtr[1] & 1);

            bufferPtr++;
        }

        Count = (int)(outputBufferPtr - RawItems + 1);
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
        RawItems = (long*)_releaseItems.GetStringStartPtr();
    }

    public long First => *RawItems;
    
    public long Pop()
    {
        var val = *RawItems;
        RawItems++;
        Count--;
        return val;
    }

    public void EnsureCapacity(int requiredSize)
    {
        if (requiredSize <= Capacity)
            return;
        
        GrowListUnlikely(requiredSize - Capacity);
    }
}
