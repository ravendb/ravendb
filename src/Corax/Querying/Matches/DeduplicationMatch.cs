using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;

namespace Corax.Querying.Matches;

public struct DeduplicationMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private TInner _inner;
    private GrowableHashSet<long> _ids;
    
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;
    
    public DeduplicationMatch(TInner inner)
    {
        _ids = new();
        _inner = inner;
    }

    public long Count => _inner.Count;
    public SkipSortingResult AttemptToSkipSorting() => _inner.AttemptToSkipSorting();

    public QueryCountConfidence Confidence => _inner.Confidence;
    public bool IsBoosting => _inner.IsBoosting;

    public int Fill(Span<long> matches)
    {
        var newResults = 0;
        int read;
        ref var startBuffer = ref MemoryMarshal.GetReference(matches);
        ref var inner = ref _inner;
        do
        {
            read = inner.Fill(matches);
            for (int i = 0; i < read; ++i)
            {
                var currentId = Unsafe.Add(ref startBuffer, i);
                Unsafe.Add(ref startBuffer, newResults) = currentId;
                newResults += _ids.Add(currentId).ToInt32();
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
