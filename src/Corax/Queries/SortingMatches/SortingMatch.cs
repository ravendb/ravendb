using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Corax.Queries.SortingMatches.Comparers;
using Corax.Utils;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils.VxSort;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Impl;

namespace Corax.Queries.SortingMatches;

[DebuggerDisplay("{DebugView,nq}")]
public unsafe partial struct SortingMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private readonly IndexSearcher _searcher;
    private readonly TInner _inner;
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
            var memoizer = match._searcher.Memoize(match._inner);
            var allMatches = memoizer.FillAndRetrieve();
            match.TotalResults = allMatches.Length;
            switch (allMatches.Length)
            {
                case 0:
                    match._results.Count = 0;
                    return 0;
                // case <= 4096:
                //     SortSmallResult<TEntryComparer>(ref match, allMatches);
                //     break;
                default:
                    SortLargeResult<TEntryComparer>(ref match, allMatches);
                    break;
            }
            memoizer.Dispose();
        }

        var read = match._results.CopyTo(matches);

        if (read != 0) 
            return read;
            
        match._results.Dispose();
        match._entriesBufferScope.Dispose();

        return 0;
    }

    private ref struct SortedIndexReader
    {
        private PostingList.Iterator _postListIt;
        private Span<byte> _smallPostingListBuffer;
        private PForDecoder.DecoderState _state;
        private CompactTree.Iterator _termsIt;
        private IndexSearcher _searcher;
        private LowLevelTransaction _llt;

        public SortedIndexReader(LowLevelTransaction llt, IndexSearcher searcher, CompactTree.Iterator termsIt)
        {
            _termsIt = termsIt;
            _termsIt.Reset();
            _llt = llt;
            _searcher = searcher;
            _postListIt = default;
            _smallPostingListBuffer = default;
            _state = default;
        }

        public int Read(Span<long> sortedIds)
        {
            int currentIdx = 0;
            // here we resume the *previous* operation
            if (_state.BufferSize != 0)
            {
                ReadSmallPostingList(sortedIds, ref currentIdx);
            }
            else if (_postListIt.IsValid)
            {
                ReadLargePostingList(sortedIds, ref currentIdx);
            }

            while (currentIdx < sortedIds.Length)
            {
                if (_termsIt.MoveNext(out  var k, out var postingListId) == false)
                    break;

                var termType = (TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask;
                switch (termType)
                {
                    case TermIdMask.Single:
                        sortedIds[currentIdx++] = EntryIdEncodings.GetContainerId(postingListId);
                        break;
                    case TermIdMask.SmallPostingList:
                        var smallSetId = EntryIdEncodings.GetContainerId(postingListId);
                        _smallPostingListBuffer = Container.Get(_llt, smallSetId).ToSpan();
                        _state = new(_smallPostingListBuffer.Length);
                        ReadSmallPostingList(sortedIds, ref currentIdx);
                        break;
                    case TermIdMask.PostingList:
                        var postingList = _searcher.GetPostingList(postingListId);
                        _postListIt = postingList.Iterate();
                        ReadLargePostingList(sortedIds, ref currentIdx);
                        break;
                    default:
                        throw new OutOfMemoryException(termType.ToString());
                }
            }

            return currentIdx;
        }

        private void ReadLargePostingList(Span<long> sortedIds, ref int currentIdx)
        {
            if (_postListIt.Fill(sortedIds[currentIdx..], out var read) == false)
                _postListIt = default;
            currentIdx += read;
        }

        private void ReadSmallPostingList(Span<long> sortedIds, ref int currentIdx)
        {
            while (currentIdx< sortedIds.Length)
            {
                var read = PForDecoder.Decode(ref _state, _smallPostingListBuffer, sortedIds[currentIdx..]);
                currentIdx += read;
                if (read == 0)
                {
                    _state = default;
                    break;
                }
            }
        }
    }

    private static void SortLargeResult<TEntryComparer>(ref SortingMatch<TInner> match, Span<long> allMatches)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;

        var take = 15;
        
        var indexesScope = allocator.Allocate(SortBatchSize * sizeof(int), out ByteString bs);
        Span<int> indexesBuffer = new(bs.Ptr, SortBatchSize);
        var sortedIdsScope = allocator.Allocate( sizeof(long) * SortBatchSize * 2, out bs);
        Span<long> sortedIdBuffer = new(bs.Ptr, SortBatchSize * 2);

        var termsTree = match._searcher.GetTermsFor(match._orderMetadata.Field.FieldName);
        var reader = new SortedIndexReader(llt, match._searcher, termsTree.Iterate());
        match._results.Init();
        
        while (match._results.Count < take)
        {
            var read = reader.Read(sortedIdBuffer);
            if (read == 0)
                break;
            var sortedIds = sortedIdBuffer[..read];
            var sortedIdsByEntryId = sortedIdBuffer[read..(read * 2)];
            sortedIds.CopyTo(sortedIdsByEntryId);
            var indexes = indexesBuffer[..read];
            InitializeIndexes(indexes);
            // we effectively permute the indexes as well as the sortedIds to get a sorted list to compare
            // with the allMatches
            sortedIdsByEntryId.Sort(indexes);
            read = SortHelper.FindMatches(indexes, sortedIdsByEntryId, allMatches);
            indexes = indexes[..read];
            indexes.Sort();
            match._results.EnsureAdditionalCapacity(indexes.Length);
            // now get the *actual* matches in their sorted order
            for (int i = 0; i < indexes.Length; i++)
            {
                int idx = indexes[i];
                match._results.Append(sortedIds[idx]);
            }
        }

        sortedIdsScope.Dispose();
        indexesScope.Dispose();
    }

    private static void InitializeIndexes(Span<int> span)
    {
        for (int i = 0; i < span.Length; i++)
            span[i] = i;
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

    private static void SortSmallResult<TEntryComparer>(ref SortingMatch<TInner> match, Span<long> batchResults) 
        where TEntryComparer : struct,  IEntryComparer, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;

        var bufScope = allocator.Allocate( batchResults.Length * (sizeof(long)+sizeof(UnmanagedSpan)), out ByteString bs);
        Span<long> batchTermIds = new(bs.Ptr, batchResults.Length);
        UnmanagedSpan* termsPtr = (UnmanagedSpan*)(bs.Ptr + batchResults.Length * sizeof(long));

        // Initialize the important infrastructure for the sorting.
        TEntryComparer entryComparer = new();
        entryComparer.Init(ref match);
        match._results.Init();
            
        var pageCache = new PageLocator(llt, 1024);
        
        var indexes = entryComparer.SortBatch(ref match, llt, pageCache, batchResults, batchTermIds, termsPtr);

        match._results.Merge(entryComparer, indexes, batchResults, termsPtr);

        pageCache.Release();
        bufScope.Dispose();
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
