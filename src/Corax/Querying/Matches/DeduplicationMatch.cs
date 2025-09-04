using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Server;

namespace Corax.Querying.Matches;

public unsafe struct DeduplicationMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private TInner _inner;
    private readonly GrowableHashSet<long> _hashset;
    private BitArray _bitmap;
    private readonly delegate*<ref DeduplicationMatch<TInner>, Span<long>, int> _fillFunc;

    private unsafe struct BitArray : IDisposable
    {
        private ulong* _bits;
        private IDisposable _memoryScope;
#if DEBUG
        public bool IsValid = true;
#endif
        public BitArray(IndexSearcher indexSearcher, int numberOfEntriesPossible)
        {
            var numberOfUlongsToAllocate = numberOfEntriesPossible / 64 + (numberOfEntriesPossible % 64 == 0 ? 0 : 1);
            _memoryScope = indexSearcher.Allocator.Allocate(numberOfUlongsToAllocate * sizeof(ulong), out ByteString memory);
            memory.ToSpan<ulong>().Clear();
            _bits = (ulong*)memory.Ptr;
#if DEBUG
            IsValid = true;
#endif
        }

        public bool Add(long id)
        {
            var mask = 1UL << (int)(id & 63);
            var bucket = _bits + (int)(id >> 6);
            var result = *bucket & mask;
            *bucket |= mask;
            return result == 0;
        }

        public void Dispose()
        {
#if DEBUG
            IsValid = false;
#endif
            _memoryScope.Dispose();
        }
    }

    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public DeduplicationMatch(IndexSearcher indexSearcher, TInner inner, bool forceHashSet)
    {
        if (indexSearcher.LastEntryId + 1 <= int.MaxValue && forceHashSet == false)
        {
            _bitmap = new BitArray(indexSearcher, (int)indexSearcher.LastEntryId + 1);
            _fillFunc = &FillViaBitmap;
        }
        else
        {
            _hashset = new GrowableHashSet<long>();
            _fillFunc = &FillViaHashSet;
        }

        _inner = inner;
    }

    public int Fill(Span<long> matches) => _fillFunc(ref this, matches);

    public long Count => _inner.Count;
    public SkipSortingResult AttemptToSkipSorting() => _inner.AttemptToSkipSorting();

    public QueryCountConfidence Confidence => _inner.Confidence;
    public bool IsBoosting => _inner.IsBoosting;

    [Conditional("DEBUG")]
    private void AssertBitmapContainer()
    {
#if DEBUG
        if (_bitmap.IsValid == false)
            throw new InvalidOperationException("Small ids container is not valid.");
#endif
    }

    private static int FillViaBitmap(ref DeduplicationMatch<TInner> match, Span<long> matches)
    {
        match.AssertBitmapContainer();

        var newResults = 0;
        int read;
        ref var startBuffer = ref MemoryMarshal.GetReference(matches);
        ref var inner = ref match._inner;
        do
        {
            read = inner.Fill(matches);
            for (int i = 0; i < read; ++i)
            {
                var currentId = Unsafe.Add(ref startBuffer, i);
                Unsafe.Add(ref startBuffer, newResults) = currentId;
                newResults += match._bitmap.Add(currentId).ToInt32();
            }
        } while (read > 0 && newResults == 0);


        // No more results. Can safely dispose the bit array.
        if (read == 0)
            match._bitmap.Dispose();

        return newResults;
    }

    private static int FillViaHashSet(ref DeduplicationMatch<TInner> match, Span<long> matches)
    {
        var newResults = 0;
        int read;
        ref var startBuffer = ref MemoryMarshal.GetReference(matches);
        ref var inner = ref match._inner;
        do
        {
            read = inner.Fill(matches);
            for (int i = 0; i < read; ++i)
            {
                var currentId = Unsafe.Add(ref startBuffer, i);
                Unsafe.Add(ref startBuffer, newResults) = currentId;
                newResults += match._hashset.Add(currentId).ToInt32();
            }
        } while (read > 0 && newResults == 0);

        return newResults;
    }


    public int AndWith(Span<long> buffer, int matches)
    {
        throw new NotSupportedException($"{nameof(DeduplicationMatch<TInner>)} is not supposed to be used as inner match of the query.");
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor) => _inner.Score(matches, scores, boostFactor);

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(nameof(DeduplicationMatch<TInner>), new List<QueryInspectionNode>() { { new QueryInspectionNode("Inner", [_inner.Inspect()]) } });
    }
}
