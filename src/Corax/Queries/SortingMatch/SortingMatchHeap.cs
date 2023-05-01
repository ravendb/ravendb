using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow.Server;
using static Corax.Queries.SortingMatch;

namespace Corax.Queries;

public interface IEntryComparer<T>
    where T : unmanaged
{
    [Pure]
    int Compare(T x, T y);

    [Pure]
    long GetEntryId(T x);
}


public unsafe ref struct SortingMatchHeap<TComparer, T>
    where TComparer : struct, IEntryComparer<T> 
    where T : unmanaged
{
    private readonly TComparer _comparer;
    public int Count;
    private T* _entries;
    private int _totalMatches;

    public (long,string)[] DebugEntries
    {
        get
        {
            if (_comparer is not IEntryComparer<long>)
                return null;

            var c = _comparer;
            return new Span<long>(_entries, Count).ToArray()
                .Select(l => (l, ((TermsReader)(object)c).GetTermFor(l)))
                .ToArray();
        }
    }
    public SortingMatchHeap(TComparer comparer)
    {
        _comparer = comparer;
        Count = 0;
    }

    public void Set(ByteString bs)
    {
        _entries = (T*)bs.Ptr;
        _totalMatches = bs.Length / sizeof(T);
    }

    public bool CapacityIncreaseNeeded(int read)
    {
        // must happen *before* we read the end, since otherwise, Add() will trim
        return (Count + read + 16 > _totalMatches);
    }

    public void Add(T entry)
    {
        if (Count < _totalMatches)
        {
            _entries[Count] = entry;
            Count++;
            HeapUp();
        }
        else if (_comparer.Compare(_entries[0], entry) > 0)
        {
            _entries[0] = entry;
            HeapDown(Count);
        }
    }

    public void Complete(Span<long> matches)
    {
        Debug.Assert(Count <= matches.Length);
        int resultsCount = Count;
        ref long dest = ref matches[0];
        while (resultsCount > 0)
        {
            T actualKey = _entries[0];
            // copy the last entry
            resultsCount--;
            _entries[0] = _entries[resultsCount];
            HeapDown(resultsCount);
            // now we are never using it, we can overwrite 
            dest = _comparer.GetEntryId(actualKey);
            dest = ref Unsafe.Add(ref dest, 1);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void HeapUp()
    {
        var current = Count - 1;
        while (current > 0)
        {
            var parent = (current - 1) / 2;

            if(_comparer.Compare(_entries[parent], _entries[current]) > 0)
                break;

          
            (_entries[parent], _entries[current]) = (_entries[current], _entries[parent]);
            current = parent;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    void HeapDown(int endOfHeap)
    {
        var current = 0;
        int childIdx;

        while ((childIdx = 2 * current + 1) < endOfHeap)
        {
            if (childIdx + 1 < endOfHeap)
            {
                // find smallest child
                if(_comparer.Compare(_entries[childIdx], _entries[childIdx+1]) < 0)
                {
                    childIdx++;
                }
            }
            
            if(_comparer.Compare(_entries[current], _entries[childIdx]) > 0)
                break;

            (_entries[childIdx], _entries[current]) = (_entries[current], _entries[childIdx]);
            current = childIdx;
        }
    }
}
