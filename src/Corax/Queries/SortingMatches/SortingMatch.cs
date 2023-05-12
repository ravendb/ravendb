using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Queries.SortingMatches.Comparers;
using Corax.Utils;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils.VxSort;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Impl;

namespace Corax.Queries.SortingMatches;

[DebuggerDisplay("{DebugView,nq}")]
public unsafe partial struct SortingMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private readonly IndexSearcher _searcher;
    private readonly IQueryMatch _inner;
    private readonly OrderMetadata _orderMetadata;
    private readonly delegate*<ref SortingMatch<TInner>, Span<long>, int> _fillFunc;

    private const int NotStarted = -1;
        
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _entriesBufferScope;

    private Results _results;
    public long TotalResults;

    public SortingMatch(IndexSearcher searcher, in TInner inner, OrderMetadata orderMetadata, int take = -1)
    {
        _searcher = searcher;
        _inner = inner;
        _orderMetadata = orderMetadata;
        _results = new Results(searcher.Transaction.LowLevelTransaction, searcher.Allocator, take);

        TotalResults = 0;

        if (_orderMetadata.HasBoost)
        {
            _fillFunc = SortBy<EntryComparerByScore>(orderMetadata);
        }
        else
        {
            _fillFunc = _orderMetadata.FieldType switch
            {
                MatchCompareFieldType.Sequence => SortBy<EntryComparerByTerm>(orderMetadata),
                MatchCompareFieldType.Alphanumeric => SortBy<EntryComparerByTermAlphaNumeric>(orderMetadata),
                MatchCompareFieldType.Integer => SortBy<EntryComparerByLong>(orderMetadata),
                MatchCompareFieldType.Floating => SortBy<EntryComparerByDouble>(orderMetadata),
                MatchCompareFieldType.Spatial => SortBy<EntryComparerBySpatial>(orderMetadata),
                _ => throw new ArgumentOutOfRangeException(_orderMetadata.FieldType.ToString())
            };
        }
    }
        
        
    private static delegate*<ref SortingMatch<TInner>, Span<long>, int> SortBy<TEntryComparer>(OrderMetadata metadata)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
        if (metadata.Ascending)
        {
            return &Fill<TEntryComparer>;
        }

        return &Fill<Descending<TEntryComparer>>;
    }


    private static int Fill<TEntryComparer>(ref SortingMatch<TInner> match, Span<long> matches)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
        // This method should also be re-entrant for the case where we have already pre-sorted everything and 
        // we will just need to acquire via pages the totality of the results. 
        if (match._results.Count == NotStarted)
        {
            FillAndSortResults<TEntryComparer>(ref match);
        }

        var read = match._results.CopyTo(matches);

        if (read != 0) 
            return read;
            
        match._results.Dispose();
        match._entriesBufferScope.Dispose();

        return 0;
    }
        
    private static string[] DebugTerms(LowLevelTransaction llt, Span<UnmanagedSpan> terms)
    {
        using var s = new CompactKeyCacheScope(llt);
        var l = new string[terms.Length];
        for (int i = 0; i < terms.Length; i++)
        {
            var item = terms[i];
            int remainderBits = item.Address[0] >> 4;
            int encodedKeyLengthInBits = (item.Length - 1) * 8 - remainderBits;
            long dicId = PersistentDictionary.CreateDefault(llt);
            s.Key.Set(encodedKeyLengthInBits, item.ToSpan()[1..], dicId);
            l[i] = s.Key.ToString();
        }

        return l;
    }
    
    private const int SortBatchSize = 4096;

    private static void FillAndSortResults<TEntryComparer>(ref SortingMatch<TInner> match) 
        where TEntryComparer : struct,  IEntryComparer, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;

        var matchesScope = allocator.Allocate(SortBatchSize * sizeof(long), out ByteString bs);
        Span<long> matches = new(bs.Ptr, SortBatchSize);
        var termsIdScope = allocator.Allocate(SortBatchSize * sizeof(long), out bs);
        Span<long> termIds = new(bs.Ptr, SortBatchSize);
        var termsScope = allocator.Allocate(SortBatchSize * sizeof(UnmanagedSpan), out bs);
        Span<UnmanagedSpan> terms = new(bs.Ptr, SortBatchSize);
        UnmanagedSpan* termsPtr = (UnmanagedSpan*)bs.Ptr;

        // Initialize the important infrastructure for the sorting.
        TEntryComparer entryComparer = new();
        entryComparer.Init(ref match);
        match._results.Init();
            
        var pageCache = new PageLocator(llt, 1024);

        while (true)
        {
            var read = match._inner.Fill(matches);
            match.TotalResults += read;
            if (read == 0)
                break;

            var batchResults = matches[..read];
            var batchTermIds = termIds[..read];
            var batchTerms = terms[..read];

            Sort.Run(batchResults);

            Span<int> indexes = entryComparer.SortBatch(ref match, llt, pageCache, batchResults, batchTermIds, termsPtr);

            match._results.Merge(entryComparer, indexes, batchResults, batchTerms);
        }

        termsScope.Dispose();
        termsIdScope.Dispose();
        matchesScope.Dispose();
    }


    public long Count => _inner.Count;

    public QueryCountConfidence Confidence => throw new NotSupportedException();

    public bool IsBoosting => _inner.IsBoosting || _orderMetadata.FieldType == MatchCompareFieldType.Score;

    public int AndWith(Span<long> buffer, int matches)
    {
        throw new NotSupportedException($"{nameof(SortingMatch<TInner>)} does not support the operation of {nameof(AndWith)}.");
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
        return new QueryInspectionNode($"{nameof(SortingMatch)} [{_orderMetadata}]",
            children: new List<QueryInspectionNode> { _inner.Inspect()},
            parameters: new Dictionary<string, string>()
            {
                { nameof(IsBoosting), IsBoosting.ToString() },
            });
    }

    string DebugView => Inspect().ToString();
}
