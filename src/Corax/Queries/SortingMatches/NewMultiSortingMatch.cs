using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Queries.SortingMatches.Comparers;
using Corax.Utils;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.PostingLists;

namespace Corax.Queries.SortingMatches;

[DebuggerDisplay("{DebugView,nq}")]
public unsafe partial struct NewMultiSortingMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private readonly IndexSearcher _searcher;
    private readonly TInner _inner;
    private readonly OrderMetadata[] _orderMetadata;
    private readonly delegate*<ref NewMultiSortingMatch<TInner>, Span<long>, int> _fillFunc;
    private readonly IEntryComparer[] _nextComparers;

    private readonly int _take;
    private const int NotStarted = -1;
        
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _entriesBufferScope;

    private NativeIntegersList _results;
    public long TotalResults;
    public bool DoNotSortResults() => throw new NotSupportedException();

    public NewMultiSortingMatch(IndexSearcher searcher, in TInner inner, OrderMetadata[] orderMetadata, int take = -1)
    {
        _searcher = searcher;
        _inner = inner;
        _orderMetadata = orderMetadata;
        _take = take;
        _results = new NativeIntegersList(searcher.Allocator);

        TotalResults = NotStarted;
        AssertNoScoreInnerComparer(orderMetadata);
        _fillFunc = SortBy(orderMetadata);

        if (orderMetadata.Length > 3)
        {
            _nextComparers = new IEntryComparer[orderMetadata.Length - 3];
            for (int comparerId = 3; comparerId < orderMetadata.Length; ++comparerId)
            {
                _nextComparers[comparerId - 3] = orderMetadata[comparerId].FieldType switch
                {
                    MatchCompareFieldType.Alphanumeric => new EntryComparerByTermAlphaNumeric(),
                    MatchCompareFieldType.Floating => new EntryComparerByDouble(),
                    MatchCompareFieldType.Integer => new EntryComparerByLong(),
                    MatchCompareFieldType.Sequence => new EntryComparerByTerm(),
                    MatchCompareFieldType.Spatial => new EntryComparerBySpatial(),
                    _ => throw new NotSupportedException()
                };

                if (orderMetadata[comparerId].Ascending == false)
                    _nextComparers[comparerId - 3] = new Descending(_nextComparers[comparerId - 3]);
            }
        }
        else
            _nextComparers = Array.Empty<IEntryComparer>();
    }

    private void AssertNoScoreInnerComparer(in OrderMetadata[] orderMetadata)
    {
        //In Corax's implementation of ranking, we assume that the IDs in the `Score()` parameter are sorted.
        //This way, we can perform a BinarySearch to find the pair <Entry, Score> and append the score to it.
        //In the case of compound sorting, when boosting is not the main comparator, matches are not sorted because the previous comparator may have changed the order.
        //This would require a linear search, which can be extremely costly. Additionally, this would require changing the API of Scoring to indicate whether it's ordered or not.
        for (int comparerId = 0; comparerId < orderMetadata.Length; ++comparerId)
        {
            if (orderMetadata[comparerId].FieldType is MatchCompareFieldType.Score && comparerId != 0)
            {
                throw new NotSupportedException(
                    $"{nameof(NewMultiSortingMatch)} can compare score only as main property. Queries like 'order by Field, [..], score(), [..] ' etc are not supported.");
            }
        }
    }

    private struct NoIterationOptimization : ITreeIterator
    {
        public void Init<T>(T parent)
        {
            throw new NotSupportedException();
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        public bool MoveNext(out long value)
        {
            throw new NotSupportedException();
        }

        public int Fill(Span<long> results)
        {
            throw new NotSupportedException();
        }
    }
    
    private static int Fill<TComparer1, TComparer2, TComparer3>(ref NewMultiSortingMatch<TInner> match, Span<long> matches)
        where TComparer1 : struct, IEntryComparer, IComparer<UnmanagedSpan> 
        where TComparer2 : struct, IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan> 
        where TComparer3 : struct, IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>  
    
    {
        // This method should also be re-entrant for the case where we have already pre-sorted everything and 
        // we will just need to acquire via pages the totality of the results. 
        if (match.TotalResults == NotStarted)
        {
            if (match._inner is not MemoizationMatch memoizer)
            {
                memoizer = match._searcher.Memoize(match._inner).Replay();
            }

            var allMatches = memoizer.FillAndRetrieve();
            match.TotalResults = allMatches.Length;
            
            if (match.TotalResults == 0)
                return 0;
            
            SortResults<TComparer1, TComparer2, TComparer3>(ref match, allMatches);
        }

        var read = match._results.MoveTo(matches);

        if (read != 0) 
            return read;
            
        match._results.Dispose();
        match._entriesBufferScope.Dispose();

        return 0;
    }
    
    private static unsafe void SortResults<TComparer1, TComparer2, TComparer3>(ref NewMultiSortingMatch<TInner> match, Span<long> batchResults) 
        where TComparer1 : struct,  IEntryComparer, IComparer<UnmanagedSpan>
        where TComparer2 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer3 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;

        var bufScope = allocator.Allocate( batchResults.Length * (sizeof(long)+sizeof(UnmanagedSpan)), out ByteString bs);
        Span<long> batchTermIds = new(bs.Ptr, batchResults.Length);
        UnmanagedSpan* termsPtr = (UnmanagedSpan*)(bs.Ptr + batchResults.Length * sizeof(long));

        // Initialize the important infrastructure for the sorting.
        TComparer1 entryComparer = new();
        entryComparer.Init(ref match, default, 0);
        var pageCache = new PageLocator(llt, 1024);
        fixed (long* ptr = batchResults)
        {
            var batchResultsAsPtr = new UnmanagedSpan<long>(ptr, sizeof(long)* batchResults.Length);
            var comp2 = new TComparer2();
            comp2.Init(ref match, batchResultsAsPtr, 1);
            var comp3 = new TComparer3();
            comp3.Init(ref match, batchResultsAsPtr, 2);

            int cmprId = 3;
            for (int index = 0; index < match._nextComparers.Length; index++)
            {
                IEntryComparer add = match._nextComparers[index];
                add.Init(ref match, batchResultsAsPtr, cmprId++);
            }

            entryComparer.SortBatch(ref match, llt, pageCache, batchResultsAsPtr, batchTermIds, termsPtr, match._orderMetadata, comp2, comp3);
        }

        pageCache.Release();
        bufScope.Dispose();
    }


    public long Count => _inner.Count;

    public QueryCountConfidence Confidence => throw new NotSupportedException();

    public bool IsBoosting => _inner.IsBoosting || _orderMetadata[0].FieldType == MatchCompareFieldType.Score;

    public int AndWith(Span<long> buffer, int matches)
    {
        throw new NotSupportedException($"{nameof(NewMultiSortingMatch<TInner>)} does not support the operation of {nameof(AndWith)}.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Fill(Span<long> matches)
    {
        return _fillFunc(ref this, matches);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Score(Span<long> matches, Span<float> scores, float boostFactor) 
    {
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(NewMultiSortingMatch)} [{_orderMetadata}]",
            children: new List<QueryInspectionNode> { _inner.Inspect()},
            parameters: new Dictionary<string, string>()
            {
                { nameof(IsBoosting), IsBoosting.ToString() },
            });
    }

    string DebugView => Inspect().ToString();
}
