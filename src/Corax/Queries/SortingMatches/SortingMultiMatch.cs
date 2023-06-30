using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Queries.SortingMatches.Meta;
using System.Threading;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data.PostingLists;

namespace Corax.Queries.SortingMatches;

[DebuggerDisplay("{DebugView,nq}")]
public unsafe partial struct SortingMultiMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private const int NextComparerOffset = 3;
    private readonly IndexSearcher _searcher;
    private readonly TInner _inner;
    private readonly OrderMetadata[] _orderMetadata;
    private readonly delegate*<ref SortingMultiMatch<TInner>, Span<long>, int> _fillFunc;
    private readonly IEntryComparer[] _nextComparers;
    private readonly int _take;
    private readonly CancellationToken _token;
    private const int NotStarted = -1;
        
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _entriesBufferScope;

    private NativeIntegersList _results;

    private SortingDataTransfer _sortingDataTransfer;
    private NativeUnmanagedList<SpatialResult> _distancesResults;
    private NativeUnmanagedList<float> _scoresResults;
    
    public long TotalResults;
    public bool DoNotSortResults() => throw new NotSupportedException();

    public SortingMultiMatch(IndexSearcher searcher, in TInner inner, OrderMetadata[] orderMetadata, int take = -1, in CancellationToken token = default)
    {
        _searcher = searcher;
        _inner = inner;
        _orderMetadata = orderMetadata;
        _take = take;
        _token = token;
        _results = new NativeIntegersList(searcher.Allocator);
        TotalResults = NotStarted;
        AssertNoScoreInnerComparer(orderMetadata);
        _fillFunc = SortBy(orderMetadata);
        
        _nextComparers = orderMetadata.Length > NextComparerOffset 
            ? HandleNextComparers() 
            : Array.Empty<IEntryComparer>();

        IEntryComparer[] HandleNextComparers()
        {
            var nextComparers = new IEntryComparer[orderMetadata.Length - NextComparerOffset];
            for (int metadataId = NextComparerOffset; metadataId < orderMetadata.Length; ++metadataId)
            {
                nextComparers[metadataId - NextComparerOffset] = (orderMetadata[metadataId].Ascending, orderMetadata[metadataId].FieldType) switch
                {
                    
                    (true, MatchCompareFieldType.Alphanumeric) => new EntryComparerByTermAlphaNumeric(),
                    (false, MatchCompareFieldType.Alphanumeric) => new Descending<EntryComparerByTermAlphaNumeric>(),

                    (true, MatchCompareFieldType.Floating) => new EntryComparerByDouble(),
                    (false, MatchCompareFieldType.Floating) => new Descending<EntryComparerByDouble>(),
                    
                    (true, MatchCompareFieldType.Integer) => new EntryComparerByLong(),
                    (false, MatchCompareFieldType.Integer) => new Descending<EntryComparerByLong>(),
                    
                    (true, MatchCompareFieldType.Sequence) => new EntryComparerByTerm(),
                    (false, MatchCompareFieldType.Sequence) => new Descending<EntryComparerByTerm>(),
                    
                    (true, MatchCompareFieldType.Spatial) => new EntryComparerBySpatial(),
                    (false, MatchCompareFieldType.Spatial) => new Descending<EntryComparerBySpatial>(),
                    _ => throw new NotSupportedException($"Ascending: {orderMetadata[metadataId].Ascending} | FieldType: {orderMetadata[metadataId].FieldType}.")
                };
            }

            return nextComparers;
        }
    }

    public void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer)
    {
        _sortingDataTransfer = sortingDataTransfer;
        if (sortingDataTransfer.IncludeDistances)
            _distancesResults = new(_searcher.Allocator);
        if (sortingDataTransfer.IncludeScores)
            _scoresResults = new(_searcher.Allocator);
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
                throw new NotSupportedException(ScoreComparerAsInnerExceptionMessage);
            }
        }
    }

    private static int Fill<TComparer1, TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match, Span<long> matches)
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
            
            match._token.ThrowIfCancellationRequested();
            var allMatches = memoizer.FillAndRetrieve();
            match.TotalResults = allMatches.Length;
            
            if (match.TotalResults == 0)
                return 0;
            
            SortResults<TComparer1, TComparer2, TComparer3>(ref match, allMatches);
        }

        var read = match._results.MoveTo(matches);
        match._scoresResults.MoveTo(match._sortingDataTransfer.ScoresBuffer);
        match._distancesResults.MoveTo(match._sortingDataTransfer.DistancesBuffer);

        if (read != 0) 
            return read;
            
        match._results.Dispose();
        match._scoresResults.Dispose();
        match._distancesResults.Dispose();
        match._entriesBufferScope.Dispose();

        return 0;
    }
    
    private static unsafe void SortResults<TComparer1, TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match, Span<long> batchResults) 
        where TComparer1 : struct,  IEntryComparer, IComparer<UnmanagedSpan>
        where TComparer2 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer3 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;
        
        var sizeToAllocate = batchResults.Length * (sizeof(long) + sizeof(UnmanagedSpan));
        //OrderBySpatial relay on this order of data. If you change it please review Spatial ordering to ensure that everything works fine. [[ids], [terms], [spatial_distances]]
        if (match._sortingDataTransfer.IncludeDistances)
            sizeToAllocate += batchResults.Length * sizeof(SpatialResult);
        
        var bufScope = allocator.Allocate(sizeToAllocate, out ByteString bs);
        Span<long> batchTermIds = new(bs.Ptr, batchResults.Length);
        UnmanagedSpan* termsPtr = (UnmanagedSpan*)(bs.Ptr + batchResults.Length * sizeof(long));

        // Initialize the important infrastructure for the sorting.
        TComparer1 entryComparer = new();
        entryComparer.Init(ref match, default, 0);
        var pageCache = new PageLocator(llt, 1024);
        fixed (long* ptrBatchResults = batchResults)
        {
            var resultsPtr = new UnmanagedSpan<long>(ptrBatchResults, sizeof(long)* batchResults.Length);
            var comp2 = new TComparer2();
            comp2.Init(ref match, resultsPtr, 1);
            var comp3 = new TComparer3();
            comp3.Init(ref match, resultsPtr, 2);
            
            for (int comparerId = 0; comparerId < match._nextComparers.Length; comparerId++)
            {
                IEntryComparer add = match._nextComparers[comparerId];
                add.Init(ref match, resultsPtr, NextComparerOffset + comparerId);
            }

            entryComparer.SortBatch(ref match, llt, pageCache, resultsPtr, batchTermIds, termsPtr, match._orderMetadata, comp2, comp3);
        }

        pageCache.Release();
        bufScope.Dispose();
    }


    public long Count => _inner.Count;

    public QueryCountConfidence Confidence => throw new NotSupportedException();

    public bool IsBoosting => _inner.IsBoosting || _orderMetadata[0].FieldType == MatchCompareFieldType.Score;

    public int AndWith(Span<long> buffer, int matches)
    {
        throw new NotSupportedException($"{nameof(SortingMultiMatch<TInner>)} does not support the operation of {nameof(AndWith)}.");
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
        return new QueryInspectionNode($"{nameof(SortingMultiMatch)} [{_orderMetadata}]",
            children: new List<QueryInspectionNode> { _inner.Inspect()},
            parameters: new Dictionary<string, string>()
            {
                { nameof(IsBoosting), IsBoosting.ToString() },
            });
    }

    string DebugView => Inspect().ToString();
    private static string ScoreComparerAsInnerExceptionMessage => $"{nameof(SortingMultiMatch)} can compare score only as main property. Queries like 'order by Field, [..], score(), [..] ' etc are not supported.";
}
