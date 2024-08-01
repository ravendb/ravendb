using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Server;
using Voron.Util;

namespace Corax.Querying.Matches.SortingMatches;

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

    private ContextBoundNativeList<long> _results;

    private SortingDataTransfer _sortingDataTransfer;
    private ContextBoundNativeList<SpatialResult> _distancesResults;
    private ContextBoundNativeList<float> _scoresResults;
    
    // This is data persisted for holding score from secondary comparer.
    private UnmanagedSpan<float> _secondaryScoreBuffer;
    private IDisposable _scoreBufferHandler;
    
    private int _alreadyReadIdx;


    public long TotalResults;
    public SkipSortingResult AttemptToSkipSorting() => throw new NotSupportedException();

    public SortingMultiMatch(IndexSearcher searcher, in TInner inner, OrderMetadata[] orderMetadata, int take = -1, in CancellationToken token = default)
    {
        _searcher = searcher;
        _inner = inner;
        _orderMetadata = orderMetadata;
        _take = take;
        _token = token;
        _alreadyReadIdx = 0;
        _results = new ContextBoundNativeList<long>(searcher.Allocator);
        TotalResults = NotStarted;
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
                    
                    (true, MatchCompareFieldType.Score) => new EntryComparerByScore(),
                    (false, MatchCompareFieldType.Score) => new Descending<EntryComparerByScore>(),
                    
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

        var read = match._results.CopyTo(matches, match._alreadyReadIdx);
        match._distancesResults.CopyTo(match._sortingDataTransfer.DistancesBuffer, match._alreadyReadIdx, read);
        match._scoresResults.CopyTo(match._sortingDataTransfer.ScoresBuffer, match._alreadyReadIdx, read);
        
        if (read != 0)
        {
            match._alreadyReadIdx += read;
            return read;
        }

        match._alreadyReadIdx = 0;

        match._results.Dispose();
        match._scoresResults.Dispose();
        match._distancesResults.Dispose();
        match._entriesBufferScope.Dispose();
        
        return 0;
    }
    
    private static void SortResults<TComparer1, TComparer2, TComparer3>(ref SortingMultiMatch<TInner> match, Span<long> matches) 
        where TComparer1 : struct,  IEntryComparer, IComparer<UnmanagedSpan>
        where TComparer2 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer3 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;
        var take = matches.Length;
        //We supports take == -1 when it means "sort all", so then take will be size of result from TInner
        // var take = Math.Min(match._take, matches.Length);
        // take = take < 0 ? matches.Length : take;
        
        var sizeToAllocate = take * (sizeof(long) + sizeof(UnmanagedSpan));
        //OrderBySpatial relay on this order of data. If you change it please review Spatial ordering to ensure that everything works fine. [[ids], [terms], [spatial_distances]]
        if (match._sortingDataTransfer.IncludeDistances)
            sizeToAllocate += take * sizeof(SpatialResult);
        
        var bufScope = allocator.Allocate(sizeToAllocate, out ByteString bs);
        Span<long> matchesTermIds = new(bs.Ptr, take);
        UnmanagedSpan* termsPtr = (UnmanagedSpan*)(bs.Ptr + take * sizeof(long));

        // Initialize the important infrastructure for the sorting.
        TComparer1 entryComparer = new();
        entryComparer.Init(ref match, default, 0);
        var pageCache = llt.PageLocator;
        fixed (long* ptrBatchResults = matches)
        {
            var resultsPtr = new UnmanagedSpan<long>(ptrBatchResults, sizeof(long)* matches.Length);
            var comp2 = new TComparer2();
            comp2.Init(ref match, resultsPtr, 1);
            var comp3 = new TComparer3();
            comp3.Init(ref match, resultsPtr, 2);
            
            for (int comparerId = 0; comparerId < match._nextComparers.Length; comparerId++)
            {
                IEntryComparer add = match._nextComparers[comparerId];
                add.Init(ref match, resultsPtr, NextComparerOffset + comparerId);
            }

            entryComparer.SortBatch(ref match, llt, pageCache, resultsPtr, matchesTermIds, termsPtr, match._orderMetadata, comp2, comp3);
        }

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
        var parameters = new Dictionary<string, string>()
        {
            {Constants.QueryInspectionNode.IsBoosting, IsBoosting.ToString()},
            {Constants.QueryInspectionNode.Count, "0"},
            {Constants.QueryInspectionNode.CountConfidence, QueryCountConfidence.Low.ToString()},
        };

        for (int cmpId = 0; cmpId < _orderMetadata.Length; ++cmpId)
        {
            ref var order = ref _orderMetadata[cmpId];
            var prefix = Constants.QueryInspectionNode.Comparer + cmpId.ToString() + "_";

            parameters.Add(prefix+Constants.QueryInspectionNode.FieldName, order.Field.FieldName.ToString());
            parameters.Add(prefix+Constants.QueryInspectionNode.Ascending, order.Ascending.ToString());
            parameters.Add(prefix+Constants.QueryInspectionNode.FieldType, order.FieldType.ToString());
            
            switch (order.FieldType)
            {
                case MatchCompareFieldType.Spatial:
                    parameters.Add(Constants.QueryInspectionNode.Point, order.Point.ToString());
                    parameters.Add(Constants.QueryInspectionNode.Round, order.Round.ToString(CultureInfo.InvariantCulture));
                    parameters.Add(Constants.QueryInspectionNode.Units, order.Units.ToString());
                    break;
                case MatchCompareFieldType.Random:
                    parameters.Add(Constants.QueryInspectionNode.RandomSeed, order.RandomSeed.ToString());
                    break;
            }
        }
        
        return new QueryInspectionNode($"{nameof(SortingMultiMatch)}",
            children: new List<QueryInspectionNode> { _inner.Inspect()},
            parameters: parameters);
    }

    string DebugView => Inspect().ToString();
}
