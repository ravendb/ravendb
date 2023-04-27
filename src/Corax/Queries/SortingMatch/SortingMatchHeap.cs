using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using static Corax.Queries.SortingMatch;

namespace Corax.Queries;

public unsafe ref struct SortingMatchHeap<TComparer>
    where TComparer : struct, IComparer<long>
{
    private readonly TComparer _comparer;
    public int Count;
    private long* _entries;
    private float* _scores;
    private int _totalMatches;

    public (long,string)[] DebugEntries
    {
        get
        {
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

    public void Set(long* entries, float* scores, int totalMatches)
    {
        _entries = entries;
        _scores = scores;
        _totalMatches = totalMatches;
    }

    public bool CapacityIncreaseNeeded(int read)
    {
        // must happen *before* we read the end, since otherwise, Add() will trim
        return (Count + read + 16 > _totalMatches);
    }


    public void Add(long entryId, float score)
    {
        if (Count < _totalMatches)
        {
            _entries[Count] = entryId;
            if (typeof(TComparer) == typeof(BoostingComparer))
            {
                _scores[Count] = score;
            }
            Count++;
            HeapUp();
        }
        else if (Compare(entryId, score) > 0)
        {
            _entries[0] = entryId;
            if (typeof(TComparer) == typeof(BoostingComparer))
            {
                _scores[0] = score;
            }

            HeapDown(Count);
        }

    }

    private int Compare(int xIdx, int yIdx)
    {
        return typeof(TComparer) == typeof(BoostingComparer) ? 
            _scores[xIdx].CompareTo(yIdx) :
            _comparer.Compare(_entries[xIdx], _entries[yIdx]);
    }

    private int Compare(long entryId, float score)
    {
        return typeof(TComparer) == typeof(BoostingComparer) ? 
            _scores[0].CompareTo(score) : 
            _comparer.Compare(_entries[0], entryId);
    }

    public void Complete(Span<long> matches)
    {
        Debug.Assert(Count <= matches.Length);
        int resultsCount = Count;
        ref long dest = ref matches[0];
        while (resultsCount > 0)
        {
            long actualKey = _entries[0];
            // copy the last entry
            resultsCount--;
            _entries[0] = _entries[resultsCount];
            HeapDown(resultsCount);
            // now we are never using it, we can overwrite 
            dest = actualKey;
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
            if (Compare(parent, current) > 0)
                break;

            if (typeof(TComparer) == typeof(BoostingComparer))
            {
                (_scores[parent], _scores[current]) = (_scores[current], _scores[parent]);
            }
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
                if (Compare(childIdx, childIdx+1) < 0)
                {
                    childIdx++;
                }
            }

            if (Compare(current, childIdx) > 0)
                break;

            if (typeof(TComparer) == typeof(BoostingComparer))
            {
                (_scores[childIdx], _scores[current]) = (_scores[current], _scores[childIdx]);
            }
            (_entries[childIdx], _entries[current]) = (_entries[current], _entries[childIdx]);
            current = childIdx;
        }
    }
}
