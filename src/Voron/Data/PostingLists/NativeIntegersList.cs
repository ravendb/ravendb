using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Server.Utils.VxSort;

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
    
    
    public void AddUnsafe(long value)
    {
        Debug.Assert(Count + 1 <= Capacity);
        RawItems[Count++] = value;
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
        
        // blog post explaining this
        // https://ayende.com/blog/200065-B/optimizing-a-three-way-merge?key=67d6f65d63ba4fb79d31dfc49ae5aa1d
        
        // The idea here is that we can do all of the process with no branches at all and make this 
        // easily predictable to the CPU

        // Here we rely on the fact that the removals has been set with 1 at the bottom bit
        // so existing / additions values would always sort *before* the removals
        var outputBufferPtr = RawItems;

        var bufferPtr = outputBufferPtr;
        var bufferEndPtr = bufferPtr + Count - 1;
        Debug.Assert((*bufferPtr & 1) == 0,
            "Removal as first item means that we have an orphaned removal, not supposed to happen!");
        while (bufferPtr < bufferEndPtr)
        {
            // here we check equality without caring if this is removal or not, skipping moving
            // to the next value if this it is the same entry twice
            outputBufferPtr += ((bufferPtr[1] & ~1) != bufferPtr[0]).ToInt32();
            *outputBufferPtr = bufferPtr[1];
            // here we check if the entry is a removal, in which can we _decrement_ the position
            // in effect, removing it
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
        Capacity--;
        return val;
    }

    public void EnsureCapacity(int requiredSize)
    {
        if (requiredSize <= Capacity)
            return;
        
        GrowListUnlikely(requiredSize - Capacity);
    }

    public Enumerator GetEnumerator() => new Enumerator(RawItems, Count);
    
    public struct Enumerator : IEnumerator<long>
    {
        private readonly long* _items;
        private int _len;
        private int _index;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Enumerator(long* items, int len)
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

        public long Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _items[_index];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
        }
    }

}
