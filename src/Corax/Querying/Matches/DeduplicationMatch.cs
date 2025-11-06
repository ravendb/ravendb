using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Collections;

namespace Corax.Querying.Matches;

public unsafe struct DeduplicationMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private bool _isGrowableBitArray = false;
    private TInner _inner;
    private readonly GrowableHashSet<long> _hashset;
    private GrowableBitArray _bitmap;
    private readonly delegate*<ref DeduplicationMatch<TInner>, Span<long>, int> _fillFunc;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public DeduplicationMatch(IndexSearcher indexSearcher, TInner inner, bool forceHashSet)
    {
        long maxBitId = (indexSearcher.LastEntryId + 1);
        long numberOfEntries = indexSearcher.NumberOfEntries;
        var bitmapMemoryRequiredInBytes = (maxBitId / 64 + (maxBitId % 64 != 0).ToInt32()) * sizeof(ulong);
        var entriesMemoryRequiredInBytes = (numberOfEntries / 64 + (numberOfEntries % 64 != 0).ToInt32()) * sizeof(ulong);

        // If the bitmap is big enough and actually only around 1.5% entries exist, we will use a HashSet instead.
        if (bitmapMemoryRequiredInBytes > IndexSearcher.BitmapMemoryRequiredThresholdInBytes 
            && entriesMemoryRequiredInBytes < (bitmapMemoryRequiredInBytes / 64) || forceHashSet)
        {
            _hashset = new GrowableHashSet<long>();
            _fillFunc = &FillViaHashSet;
        }
        else
        {
            _bitmap = new GrowableBitArray(indexSearcher.Allocator, (int)indexSearcher.LastEntryId);
            _fillFunc = &FillViaBitmap;
            _isGrowableBitArray = true;
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
        // We fetch data until we encounter the first unseen document(s). Duplicates are uncommon,
        // and often we don't need the exact number of results. It's better to let the caller decide
        // whether to continue, since it has better knowledge of the query.

        // No more results. We can dispose the bitmap.
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
        // We fetch data until we encounter the first unseen document(s). Duplicates are uncommon,
        // and often we don't need the exact number of results. It's better to let the caller decide
        // whether to continue, since it has better knowledge of the query.
        
        
        return newResults;
    }


    public int AndWith(Span<long> buffer, int matches)
    {
        throw new NotSupportedException($"{nameof(DeduplicationMatch<TInner>)} is not supposed to be used as inner match of the query.");
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor) => _inner.Score(matches, scores, boostFactor);

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(nameof(DeduplicationMatch<TInner>),
            parameters: new Dictionary<string, string>()
            {
                {"DataStructure", _isGrowableBitArray ? "Bitmap" : "HashSet"}
            },
            children: [_inner.Inspect()]);
    }
}
