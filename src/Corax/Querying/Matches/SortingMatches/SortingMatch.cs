using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Indexing;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;

namespace Corax.Querying.Matches.SortingMatches;

[DebuggerDisplay("{DebugView,nq}")]
public unsafe partial struct SortingMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private readonly IndexSearcher _searcher;
    private readonly TInner _inner;
    private readonly OrderMetadata _orderMetadata;
    private readonly CancellationToken _cancellationToken;
    private readonly delegate*<ref SortingMatch<TInner>, Span<long>, int> _fillFunc;
    private readonly int _take;
    private const int NotStarted = -1;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _entriesBufferScope;

    private ContextBoundNativeList<long> _results;
    private ContextBoundNativeList<SpatialResult> _distancesResults;
    private ContextBoundNativeList<float> _scoresResults;
    private int _alreadyReadIdx;
    
    
    private SortingDataTransfer _sortingDataTransfer;
    public long TotalResults;
    public SkipSortingResult AttemptToSkipSorting() => throw new NotSupportedException();

    public SortingMatch(IndexSearcher searcher, in TInner inner, OrderMetadata orderMetadata, in CancellationToken cancellationToken, int take = -1)
    {
        _searcher = searcher;
        _inner = inner;
        _orderMetadata = orderMetadata;
        _cancellationToken = cancellationToken;
        _take = take;
        _alreadyReadIdx = 0;
        _results = new ContextBoundNativeList<long>(searcher.Allocator);
        TotalResults = NotStarted;

        if (_orderMetadata.HasBoost)
        {
            _fillFunc = SortBy<EntryComparerByScore, NoIterationOptimization, NoIterationOptimization>(orderMetadata);
        }
        else
        {
            _fillFunc = _orderMetadata.FieldType switch
            {
                MatchCompareFieldType.Sequence => SortBy<EntryComparerByTerm, Lookup<CompactTree.CompactKeyLookup>.ForwardIterator,  Lookup<CompactTree.CompactKeyLookup>.BackwardIterator>(orderMetadata),
                MatchCompareFieldType.Alphanumeric => SortBy<EntryComparerByTermAlphaNumeric, NoIterationOptimization, NoIterationOptimization>(orderMetadata),
                MatchCompareFieldType.Integer => SortBy<EntryComparerByLong, Lookup<Int64LookupKey>.ForwardIterator, Lookup<Int64LookupKey>.BackwardIterator>(orderMetadata),
                MatchCompareFieldType.Floating => SortBy<EntryComparerByDouble,  Lookup<DoubleLookupKey>.ForwardIterator, Lookup<DoubleLookupKey>.BackwardIterator>(orderMetadata),
                MatchCompareFieldType.Spatial => SortBy<EntryComparerBySpatial, NoIterationOptimization, NoIterationOptimization>(orderMetadata),
                MatchCompareFieldType.Random => SortBy<EntryComparerByTerm,  RandomDirection, RandomDirection>(orderMetadata),
                _ => throw new ArgumentOutOfRangeException(_orderMetadata.FieldType.ToString())
            };
        }
    }
    private struct RandomDirection : ILookupIterator
    {
        
        public bool IsForward => throw new NotSupportedException($"{nameof(RandomDirection)} has no direction and should not be used in parts of code where it is required.");

        public void Init<T>(T parent)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public int Fill(Span<long> results, long lastId, bool includeMax)
        {
            throw new NotImplementedException();
        }
        
        public bool Skip(long count)
        {
            throw new NotImplementedException();
        }

        public bool MoveNext(out long value)
        {
            throw new NotImplementedException();
        }

        public bool MoveNext<TLookupKey>(out TLookupKey key, out long value, out bool hasPreviousValue)
        {
            throw new NotImplementedException();
        }

        public void Seek<TLookupKey>(TLookupKey key)
        {
            throw new NotImplementedException();
        }
    }
    
    private struct NoIterationOptimization : ILookupIterator
    {
        public bool IsForward => throw new NotSupportedException($"{nameof(NoIterationOptimization)} has no direction and should not be used in parts of code where it is required.");

        
        public void Init<T>(T parent)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public int Fill(Span<long> results, long lastId = long.MaxValue, bool includeMax = true)
        {
            throw new NotImplementedException();
        }
        
        public bool Skip(long count)
        {
            throw new NotImplementedException();
        }

        public bool MoveNext(out long value)
        {
            throw new NotImplementedException();
        }

        public bool MoveNext<TLookupKey>(out TLookupKey key, out long value, out bool hasPreviousValue)
        {
            throw new NotImplementedException();
        }

        public void Seek<TLookupKey>(TLookupKey key)
        {
            throw new NotImplementedException();
        }
    }
        
    private static delegate*<ref SortingMatch<TInner>, Span<long>, int> SortBy<TEntryComparer,TFwdIt,TBackIt>(OrderMetadata metadata)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
        where TFwdIt : struct,  ILookupIterator
        where TBackIt : struct, ILookupIterator
    {
        if (metadata.Ascending)
        {
            return &Fill<TEntryComparer, TFwdIt>;
        }

        return &Fill<Descending<TEntryComparer>, TBackIt>;
    }


    private static int Fill<TEntryComparer, TDirection>(ref SortingMatch<TInner> match, Span<long> matches)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
        where TDirection : struct, ILookupIterator
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

            const int IndexSortingThreshold = 4096;
            if (typeof(TDirection) == typeof(RandomDirection))
            {
                SortByRandom(ref match, allMatches);
            }
            else if (typeof(TDirection) == typeof(NoIterationOptimization) || 
                match.TotalResults < IndexSortingThreshold)
            {
                SortResults<TEntryComparer>(ref match, allMatches);
            }
            else
            {
                SortUsingIndex<TEntryComparer, TDirection>(ref match, allMatches);
            }
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
        match._entriesBufferScope.Dispose();
        match._scoresResults.Dispose();
        match._distancesResults.Dispose();

        return 0;
    }

    private static void SortByRandom(ref SortingMatch<TInner> match, Span<long> results)
    {
        var random = new Random(match._orderMetadata.RandomSeed);
        var take = Math.Min(match._take, results.Length);
        while (match._results.Count < take)
        {
            int index = random.Next(match._results.Count, results.Length);
            // fisher yates
            var replaced = results[match._results.Count];
            var selected = results[index];
            results[match._results.Count] = selected;
            results[index] = replaced;
            match._results.Add(selected);
        }
    }

    private ref struct SortedIndexReader<TDirection>
        where TDirection : struct, ILookupIterator
    {
        private PostingList.Iterator _postListIt;
        private FastPForBufferedReader _smallListReader;
        private TDirection _termsIt;
        private readonly long _min;
        private readonly long _max;
        private readonly Querying.IndexSearcher _searcher;
        private readonly LowLevelTransaction _llt;

        private const int BufferSize = 1024;
        private readonly long* _itBuffer;
        private readonly UnmanagedSpan* _containerItems;
        private int _bufferIdx;
        private int _bufferCount;
        private int _smallPostingListIndex;
        private ContextBoundNativeList<long> _smallPostListIds;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _itBufferScope, _containerItemsScope;
        private readonly PageLocator _pageLocator;
        private bool _hasSmallListReader;


        public SortedIndexReader(LowLevelTransaction llt, Querying.IndexSearcher searcher, TDirection it, long min, long max)
        {
            _termsIt = it;
            _min = min;
            _max = max;
            _termsIt.Reset();
            _llt = llt;
            _searcher = searcher;
            _postListIt = default;
            _smallListReader = default;
            _smallPostListIds = new ContextBoundNativeList<long>(llt.Allocator,BufferSize);
            _bufferCount = _bufferIdx = 0;
            _itBufferScope = llt.Allocator.Allocate(BufferSize * sizeof(long), out ByteString bs);
            _itBuffer = (long*)bs.Ptr;
            _containerItemsScope = llt.Allocator.Allocate(BufferSize * sizeof(UnmanagedSpan), out bs);
            _containerItems = (UnmanagedSpan*)bs.Ptr;
            _pageLocator = llt.PageLocator;
        }


        public int Read(Span<long> sortedIds)
        {
            fixed (long* pSortedIds = sortedIds)
            {
                int currentIdx = 0;
                // here we resume the *previous* operation
                if (_hasSmallListReader)
                {
                    ReadSmallPostingList(pSortedIds, sortedIds.Length, ref currentIdx);
                }
                else if (_postListIt.IsValid)
                {
                    ReadLargePostingList(sortedIds, ref currentIdx);
                }

                while (currentIdx < sortedIds.Length)
                {
                    if (_bufferIdx == _bufferCount)
                    {
                        RefillBuffers();
                        if (_bufferCount == 0)
                            break;
                    }

                    var postingListId = _itBuffer[_bufferIdx++];
                    var termType = (TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask;
                    switch (termType)
                    {
                        case TermIdMask.Single:
                            long entryId = EntryIdEncodings.GetContainerId(postingListId);
                            if(entryId >= _min && entryId <= _max)
                                sortedIds[currentIdx++] = entryId;
                            break;
                        case TermIdMask.SmallPostingList:
                            var item = _containerItems[_smallPostingListIndex++];
                            _ = VariableSizeEncoding.Read<int>(item.Address, out var offset); // discard count here
                            var start = FastPForDecoder.ReadStart(item.Address + offset);
                            if(start > _max)
                                continue;
                            if (_smallListReader.WasInitialized == false)
                            {
                                _smallListReader = new FastPForBufferedReader(_llt.Allocator);
                            }

                            _hasSmallListReader = true;
                            _smallListReader.Init(item.Address + offset, item.Length - offset);
                            ReadSmallPostingList(pSortedIds, sortedIds.Length, ref currentIdx);
                            break;
                        case TermIdMask.PostingList:
                            var postingList = _searcher.GetPostingList(postingListId);
                            _postListIt = postingList.Iterate();
                            _postListIt.Seek(_min);
                            ReadLargePostingList(sortedIds, ref currentIdx);
                            break;
                        default:
                            throw new OutOfMemoryException(termType.ToString());
                    }
                }

                return currentIdx;
            }
        }

        private void RefillBuffers()
        {
            _smallPostListIds.Clear();
            _bufferIdx = 0;
            _bufferCount = 0;
            _bufferCount = _termsIt.Fill(new Span<long>(_itBuffer, BufferSize));
            if (_bufferCount == 0)
                return;
            
            for (int i = 0; i < _bufferCount; i++)
            {
                var termType = (TermIdMask)_itBuffer[i] & TermIdMask.EnsureIsSingleMask;
                if (termType == TermIdMask.SmallPostingList)
                {
                    var smallSetId = EntryIdEncodings.GetContainerId(_itBuffer[i]);
                    _smallPostListIds.Add(smallSetId);
                }
            }

            _smallPostingListIndex = 0;
            if (_smallPostListIds.Count == 0)
                return;

            Container.GetAll(_llt, _smallPostListIds.ToSpan(), _containerItems, long.MinValue, _pageLocator);
        }

        private void ReadLargePostingList(Span<long> sortedIds, ref int currentIdx)
        {
            if (_postListIt.Fill(sortedIds[currentIdx..], out var read) == false || EntryIdEncodings.DecodeAndDiscardFrequency(sortedIds[currentIdx + read - 1]) > _max)
                _postListIt = default;

            EntryIdEncodings.DecodeAndDiscardFrequency(sortedIds.Slice(currentIdx), read);
            currentIdx += read;
        }

        private void ReadSmallPostingList(long* pSortedIds, int count, ref int currentIdx)
        {
            while (currentIdx < count)
            {
                var read = _smallListReader.Fill(pSortedIds + currentIdx, count - currentIdx);
                EntryIdEncodings.DecodeAndDiscardFrequency(new Span<long>(pSortedIds + currentIdx, read), read);
                if (read == 0)
                {
                    _hasSmallListReader = false;
                    break;
                }
                if (pSortedIds[currentIdx + read - 1] < _min)
                    continue;
                currentIdx += read;
            }
        }

        public void Dispose()
        {
            _smallListReader.Dispose();
            _smallPostListIds.Dispose();
            _containerItemsScope.Dispose();
            _itBufferScope.Dispose();
        }
    }

    private static void SortUsingIndex<TEntryComparer, TDirection>(ref SortingMatch<TInner> match, Span<long> allMatches)
        where TDirection : struct, ILookupIterator
        where TEntryComparer : struct,  IEntryComparer, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;
        var entryCmp = default(TEntryComparer);

        int maxResults = match._take == -1 ? int.MaxValue : match._take;

        var indexesScope = allocator.Allocate(SortingMatch.SortBatchSize * sizeof(long), out ByteString bs);
        Span<long> indexesBuffer = new(bs.Ptr,SortingMatch.SortBatchSize);
        var sortedIdsScope = allocator.Allocate( sizeof(long) * SortingMatch.SortBatchSize, out bs);
        Span<long> sortedIdBuffer = new(bs.Ptr, SortingMatch.SortBatchSize);

        var totalRead = 0;
        var reader = GetReader(ref match, allMatches[0], allMatches[^1]);
        while (match._results.Count < maxResults)
        {
            match._cancellationToken.ThrowIfCancellationRequested();
            if (totalRead > allMatches.Length * 2)
            {
                // We may have _already_ matched some items, in which case they show up as negative 
                // numbers in the matches (since we want to filter them), we need to pass the matches to the 
                // direct SortResult, but first we need to remove all the items that we already matched
                int notMatchedYet = FilterAlreadyFoundMatches(allMatches);

                if (notMatchedYet > 0)
                {
                    // if we scanned through the index more than twice the amount of records of the query, but still
                    // didn't find enough to fill the page size, we'll fall back to normal sorting, instead of using the
                    // index method. That would prevent degenerate cases.
                    SortResults<TEntryComparer>(ref match, allMatches[..notMatchedYet]);
                }

                return;
            }

            var read = reader.Read(sortedIdBuffer);
            if (read == 0)
            {
                // there are no more results from the index, but we may have records that don't *have* an entry here
                // in that case, we add them to the results in arbitrary order
                for (int i = 0; i < allMatches.Length; i++)
                {
                    if(allMatches[i] < 0) // meaning, it was already matched by the SortHelper
                        continue;
                    match._results.Add(allMatches[i]);
                    if (match._results.Count >= maxResults)
                        break;
                }
                break;
            }

            totalRead += read;
            var sortedIds = sortedIdBuffer[..read];
            var indexes = indexesBuffer[..read];
            // we effectively permute the indexes as well as the sortedIds to get a sorted list to compare
            // with the allMatches
            InitializeIndexesTopHalf(indexes);
            sortedIds.Sort(indexes);
            InitializeIndexesBottomHalf(indexes);
            read = SortHelper.FindMatches(indexes, sortedIds, allMatches);
            indexes = indexes[..read];
            indexes.Sort();
            // now get the *actual* matches in their sorted order
            for (int i = 0; i < indexes.Length && match._results.Count < maxResults; i++)
            {
                match._results.Add(sortedIds[(int)indexes[i]]);
            }
        }

        reader.Dispose();
        sortedIdsScope.Dispose();
        indexesScope.Dispose();
        
        
        SortedIndexReader<TDirection> GetReader(ref SortingMatch<TInner> match, long min, long max)
        {
            if (typeof(TDirection) == typeof(Lookup<CompactTree.CompactKeyLookup>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<CompactTree.CompactKeyLookup>.BackwardIterator))
            {
                var termsTree = match._searcher.GetTermsFor(entryCmp.GetSortFieldName(ref match));
                return new SortedIndexReader<TDirection>(llt, match._searcher, termsTree.IterateValues<TDirection>(), min, max);
            }

            if (typeof(TDirection) == typeof(Lookup<Int64LookupKey>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<Int64LookupKey>.BackwardIterator))
            {
                var termsTree = match._searcher.GetLongTermsFor(entryCmp.GetSortFieldName(ref match));
                return new SortedIndexReader<TDirection>(llt, match._searcher, termsTree.Iterate<TDirection>(), min, max);
            }

            if (typeof(TDirection) == typeof(Lookup<DoubleLookupKey>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<DoubleLookupKey>.BackwardIterator))
            {
                var termsTree = match._searcher.GetDoubleTermsFor(entryCmp.GetSortFieldName(ref match));
                return new SortedIndexReader<TDirection>(llt, match._searcher, termsTree.Iterate<TDirection>(), min, max);
            }

            throw new NotSupportedException(typeof(TDirection).FullName);
        }
    }
    
    private static int FilterAlreadyFoundMatches(Span<long> items)
    {
        int output = 0;
        for (int i = 0; i < items.Length; i++)
        {
            if ((items[i] & ~long.MaxValue) != 0)
                continue;
            items[output++] = items[i];
        }
        return output;
    }


    private static void InitializeIndexesTopHalf(Span<long> span)
    {
        for (int i = 0; i < span.Length; i++)
            span[i] = (long)i << 32;
    }
    
    private static void InitializeIndexesBottomHalf(Span<long> span)
    {
        for (int i = 0; i < span.Length; i++)
            span[i] |= (uint)i;
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
            long dicId = CompactTree.GetDictionaryId(llt);
            s.Key.Set(encodedKeyLengthInBits, item.ToSpan()[1..], dicId);
            l[i] = s.Key.ToString();
        }

        return l;
    }
    
    private static void SortResults<TEntryComparer>(ref SortingMatch<TInner> match, Span<long> batchResults) 
        where TEntryComparer : struct,  IEntryComparer, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;

        var sizeToAllocate = batchResults.Length * (sizeof(long) + sizeof(UnmanagedSpan));

        //OrderBySpatial relies on this order of data. If you change it, please review the spatial ordering to ensure that everything works fine: [[ids], [terms], [spatial_distances]].
        if (match._sortingDataTransfer.IncludeDistances)
            sizeToAllocate += batchResults.Length * sizeof(SpatialResult);
        
        var bufScope = allocator.Allocate(sizeToAllocate, out ByteString bs);
        Span<long> batchTermIds = new(bs.Ptr, batchResults.Length);
        UnmanagedSpan* termsPtr = (UnmanagedSpan*)(bs.Ptr + batchResults.Length * sizeof(long));

        // Initialize the important infrastructure for the sorting.
        TEntryComparer entryComparer = new();
        entryComparer.Init(ref match);
        
        entryComparer.SortBatch(ref match, llt, llt.PageLocator, batchResults, batchTermIds, termsPtr);

        bufScope.Dispose();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer)
    {
        _sortingDataTransfer = sortingDataTransfer;
        if (_sortingDataTransfer.IncludeScores)
            _scoresResults = new(_searcher.Allocator);
        if (_sortingDataTransfer.IncludeDistances)
            _distancesResults = new(_searcher.Allocator);
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
        var parameters = new Dictionary<string, string>()
        {
            {Constants.QueryInspectionNode.IsBoosting, IsBoosting.ToString()},
            {Constants.QueryInspectionNode.FieldName, _orderMetadata.Field.FieldName.ToString()},
            {Constants.QueryInspectionNode.Ascending, _orderMetadata.Ascending.ToString()},
            {Constants.QueryInspectionNode.FieldType, _orderMetadata.FieldType.ToString()},
        };

        switch (_orderMetadata.FieldType)
        {
            case MatchCompareFieldType.Spatial:
                parameters.Add(Constants.QueryInspectionNode.Point, _orderMetadata.Point.ToString());
                parameters.Add(Constants.QueryInspectionNode.Round, _orderMetadata.Round.ToString(CultureInfo.InvariantCulture));
                parameters.Add(Constants.QueryInspectionNode.Units, _orderMetadata.Units.ToString());
                break;
            case MatchCompareFieldType.Random:
                parameters.Add(Constants.QueryInspectionNode.RandomSeed, _orderMetadata.RandomSeed.ToString());
                break;
        }
        
        return new QueryInspectionNode($"{nameof(SortingMatch)}",
            children: new List<QueryInspectionNode> { _inner.Inspect()},
            parameters: parameters);
    }

    string DebugView => Inspect().ToString();
}
