using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Corax.Queries.SortingMatches.Comparers;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util.PFor;
using Voron.Util.Simd;

namespace Corax.Queries.SortingMatches;

[DebuggerDisplay("{DebugView,nq}")]
public unsafe partial struct NewMultiSortingMatch<TInner> : IQueryMatch
    where TInner : IQueryMatch
{
    private readonly IndexSearcher _searcher;
    private readonly TInner _inner;
    private readonly OrderMetadata[] _orderMetadata;
    private readonly delegate*<ref NewMultiSortingMatch<TInner>, Span<long>, int> _fillFunc;

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
        _fillFunc = SortBy(orderMetadata);
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
    
    private static int Fill<TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(ref NewMultiSortingMatch<TInner> match, Span<long> matches)
        where TComparer1 : struct, IEntryComparer, IComparer<UnmanagedSpan> 
        where TComparer2 : struct, IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan> 
        where TComparer3 : struct, IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>  
        where TComparer4 : struct, IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan> 
        where TComparer5 : struct, IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan> 
        where TComparer6 : struct, IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan> 
        where TComparer7 : struct, IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan> 
        where TComparer8 : struct, IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan> 
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
            
            SortResults<TComparer1, TComparer2, TComparer3, TComparer4 , TComparer5, TComparer6, TComparer7, TComparer8>(ref match, allMatches);
        }

        var read = match._results.MoveTo(matches);

        if (read != 0) 
            return read;
            
        match._results.Dispose();
        match._entriesBufferScope.Dispose();

        return 0;
    }

    private ref struct SortedIndexReader<TIterator>
        where TIterator : struct, ITreeIterator
    {
        private PostingList.Iterator _postListIt;
        private FastPForBufferedReader _smallListReader;
        private TIterator _termsIt;
        private readonly long _min;
        private readonly long _max;
        private readonly IndexSearcher _searcher;
        private readonly LowLevelTransaction _llt;

        private const int BufferSize = 1024;
        private readonly long* _itBuffer;
        private readonly UnmanagedSpan* _containerItems;
        private int _bufferIdx;
        private int _bufferCount;
        private int _smallPostingListIndex;
        private NativeIntegersList _smallPostListIds;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _itBufferScope, _containerItemsScope;
        private readonly PageLocator _pageLocator;

        public SortedIndexReader(LowLevelTransaction llt, IndexSearcher searcher, TIterator it, long min, long max)
        {
            _termsIt = it;
            _min = min;
            _max = max;
            _termsIt.Reset();
            _llt = llt;
            _searcher = searcher;
            _postListIt = default;
            _smallListReader = default;
            _smallPostListIds = new NativeIntegersList(llt.Allocator,BufferSize);
            _bufferCount = _bufferIdx = 0;
            _itBufferScope = llt.Allocator.Allocate(BufferSize * sizeof(long), out ByteString bs);
            _itBuffer = (long*)bs.Ptr;
            _containerItemsScope = llt.Allocator.Allocate(BufferSize * sizeof(UnmanagedSpan), out bs);
            _containerItems = (UnmanagedSpan*)bs.Ptr;
            _pageLocator = new PageLocator(llt, BufferSize);
        }


        public int Read(Span<long> sortedIds)
        {
            fixed (long* pSortedIds = sortedIds)
            {
                int currentIdx = 0;
                // here we resume the *previous* operation
                if (_smallListReader.IsValid)
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
                            _smallListReader = new FastPForBufferedReader(_llt.Allocator, item.Address + offset, item.Length - offset);
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
            Container.GetAll(_llt, _smallPostListIds.Items,_containerItems, long.MinValue, _pageLocator);
        }

        private void ReadLargePostingList(Span<long> sortedIds, ref int currentIdx)
        {
            if (_postListIt.Fill(sortedIds[currentIdx..], out var read) == false || 
                sortedIds[read-1] > _max)
                _postListIt = default;
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
                    _smallListReader.Dispose();
                    _smallListReader = default;
                    break;
                }
                if (pSortedIds[currentIdx + read - 1] < _min)
                    continue;
                currentIdx += read;
            }
        }

        public void Dispose()
        {
            _smallPostListIds.Dispose();
            _containerItemsScope.Dispose();
            _itBufferScope.Dispose();
        }
    }

    private static void SortUsingIndex<TEntryComparer,TDirection>(ref NewMultiSortingMatch<TInner> match, Span<long> allMatches) 
        where TDirection : struct, ITreeIterator
        where TEntryComparer : struct, IEntryComparer
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;
        var entryCmp = default(TEntryComparer);

        int maxResults = match._take == -1 ? int.MaxValue : match._take;

        var indexesScope = allocator.Allocate(SortBatchSize * sizeof(long), out ByteString bs);
        Span<long> indexesBuffer = new(bs.Ptr, SortBatchSize);
        var sortedIdsScope = allocator.Allocate( sizeof(long) * SortBatchSize, out bs);
        Span<long> sortedIdBuffer = new(bs.Ptr, SortBatchSize);

        var reader = GetReader(ref match, allMatches[0], allMatches[^1]);
        while (match._results.Count < maxResults)
        {
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
        
        
        SortedIndexReader<TDirection> GetReader(ref NewMultiSortingMatch<TInner> match, long min, long max)
        {
            if (typeof(TDirection) == typeof(CompactTree.ForwardIterator) ||
                typeof(TDirection) == typeof(CompactTree.BackwardIterator))
            {
                var termsTree = match._searcher.GetTermsFor(entryCmp.GetSortFieldName(ref match));
                return new SortedIndexReader<TDirection>(llt, match._searcher, termsTree.Iterate<TDirection>(), min, max);
            }

            if (typeof(TDirection) == typeof(Lookup<long>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<long>.BackwardIterator))
            {
                var termsTree = match._searcher.GetLongTermsFor(entryCmp.GetSortFieldName(ref match));
                return new SortedIndexReader<TDirection>(llt, match._searcher, termsTree.Iterate<TDirection>(), min, max);
            }

            if (typeof(TDirection) == typeof(Lookup<double>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<double>.BackwardIterator))
            {
                var termsTree = match._searcher.GetDoubleTermsFor(entryCmp.GetSortFieldName(ref match));
                return new SortedIndexReader<TDirection>(llt, match._searcher, termsTree.Iterate<TDirection>(), min, max);
            }

            throw new NotSupportedException(typeof(TDirection).FullName);
        }
        

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
            long dicId = PersistentDictionary.CreateDefault(llt);
            s.Key.Set(encodedKeyLengthInBits, item.ToSpan()[1..], dicId);
            l[i] = s.Key.ToString();
        }

        return l;
    }
    
    private const int SortBatchSize = 4096;

    private static unsafe void SortResults<TComparer1, TComparer2, TComparer3, TComparer4, TComparer5, TComparer6, TComparer7, TComparer8>(ref NewMultiSortingMatch<TInner> match, Span<long> batchResults) 
        where TComparer1 : struct,  IEntryComparer, IComparer<UnmanagedSpan>
        where TComparer2 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer3 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer4 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer5 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer6 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer7 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer8 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>

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
            var comp4 = new TComparer4();
            comp4.Init(ref match, batchResultsAsPtr, 3);
            var comp5 = new TComparer5();
            comp5.Init(ref match, batchResultsAsPtr, 4);
            var comp6 = new TComparer6();
            comp6.Init(ref match, batchResultsAsPtr, 5);
            var comp7 = new TComparer7();
            comp7.Init(ref match, batchResultsAsPtr, 6);
            var comp8 = new TComparer8();
            comp8.Init(ref match, batchResultsAsPtr, 7);
            

            entryComparer.SortBatch(ref match, llt, pageCache, batchResultsAsPtr, batchTermIds, termsPtr, match._orderMetadata, comp2, comp3, comp4, comp5, comp6, comp7, comp8);
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
