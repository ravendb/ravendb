using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Server.Platform.Win32;
using Sparrow.Utils;
using static Corax.Queries.SortingMatch;

namespace Corax.Queries;

public unsafe ref struct SortingMatchHeap<TComparer, TOut>
    where TComparer : struct, IMatchComparer
    where TOut : struct
{
    public int Count;
    private byte* _items;
    private int _totalMatches;
    private readonly MatchComparer<TComparer, TOut> _comparer;

    public MatchComparer<TComparer, TOut>.Item[] Items => new Span<MatchComparer<TComparer, TOut>.Item>(_items, Count).ToArray();

    public SortingMatchHeap(TComparer comparer, byte* buffer, int totalMatches)
    {
        _items = buffer;
        _totalMatches = totalMatches;
        _comparer = new MatchComparer<TComparer, TOut>(comparer);
        Count = 0;
    }

    public bool CapacityIncreaseNeeded(int read)
    {
        // must happen *before* we read the end, since otherwise, Add() will trim
        return (Count + read + 16 > _totalMatches);
    }

    public void IncreaseCapacity(byte* buffer, int totalMatches)
    {
        _totalMatches = totalMatches;
        _items = buffer;
    }

    public void Add(in MatchComparer<TComparer, TOut>.Item cur)
    {
        ref var itemsRef = ref Unsafe.AsRef<MatchComparer<TComparer,TOut>.Item>(_items);

        if (Count < _totalMatches)
        {
            ref var itemPtr = ref Unsafe.Add(ref itemsRef, Count);
            itemPtr = cur;
            Count++;
            HeapUp(ref itemsRef);
        }
        else if (_comparer.Compare(itemsRef, cur) > 0)
        {
            itemsRef = cur;
            HeapDown(ref itemsRef, Count);
        }
    }

    public void Complete(Span<long> matches)
    {
        Debug.Assert(Count <= matches.Length);
        ref var itemsStart = ref Unsafe.AsRef<MatchComparer<TComparer,TOut>.Item>(_items);
        int resultsCount = Count;
        ref long dest = ref Unsafe.Add(ref matches[0], Count- 1);
        while (resultsCount > 0)
        {
            // in case when value doesn't exist in entry we set entryId as -entryId in `Item`, let's revert that.
            long actualKey = itemsStart.Key < 0 ? -itemsStart.Key : itemsStart.Key;
            // copy the last entry
            resultsCount--;
            itemsStart = Unsafe.Add(ref itemsStart, resultsCount);
            HeapDown(ref itemsStart, resultsCount);
            // now we are never using it, we can overwrite 
            dest = actualKey;
            dest = ref Unsafe.Subtract(ref dest, 1);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void HeapUp(ref MatchComparer<TComparer, TOut>.Item itemsStart)
    {
        var current = Count - 1;
        while (current > 0)
        {
            var parent = (current - 1) / 2;
            ref var parentItem = ref Unsafe.Add(ref itemsStart, parent);
            ref var currentItem = ref Unsafe.Add(ref itemsStart, current);
            if (_comparer.Compare(ref parentItem, ref currentItem) > 0)
                break;

            (parentItem, currentItem) = (currentItem, parentItem);
            current = parent;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void HeapDown(ref MatchComparer<TComparer, TOut>.Item itemsPtr, int endOfHeap)
    {
        var current = 0;
        int childIdx;

        while ((childIdx = 2 * current + 1) < endOfHeap)
        {
            if (childIdx + 1 < endOfHeap)
            {
                if (_comparer.Compare( // find smallest child
                        ref Unsafe.Add(ref itemsPtr, childIdx), 
                        ref Unsafe.Add(ref itemsPtr, childIdx + 1)) < 0)
                {
                    childIdx++;
                }
            }

            ref var currentPtr = ref Unsafe.Add(ref itemsPtr, current);
            ref var childPtr = ref Unsafe.Add(ref itemsPtr, childIdx);
            if (_comparer.Compare(ref currentPtr, ref childPtr) > 0)
                break;

            (currentPtr, childPtr) = (childPtr, currentPtr);
            current = childIdx;
        }
    }
}
