using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Voron.Data.RoaringBitmaps;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Server;
using Sparrow.Server.Utils;
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
public sealed unsafe partial class SortingMatch<TInner> : SortingMatch
    where TInner : IQueryMatch
{
    private readonly IndexSearcher _searcher;
    private TInner _inner;
    private readonly OrderMetadata _orderMetadata;
    private readonly CancellationToken _cancellationToken;
    private readonly bool _nullFirst;
    private readonly delegate*<SortingMatch<TInner>, Span<long>, int> _fillFunc;
    private readonly int _take;
    private const int NotStarted = -1;

    private const int Utf8StackAllocThreshold = 256;

    /// <summary>
    /// Cost ratio of cheap streamed entries (sequential decode + bitmap test) vs. expensive materialized candidates (random lookups in GetFor, GetAll, sorting, etc).
    /// Benchmarks led to this value, which is how much more entries we should process in streaming vs. in memory sort.
    /// </summary>
    private const int IndexStreamingVsInMemorySortCostRatio = 16;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _entriesBufferScope;

    private ContextBoundNativeList<long> _results;
    private ContextBoundNativeList<SpatialResult> _distancesResults;
    private ContextBoundNativeList<float> _scoresResults;
    private int _alreadyReadIdx;


    private SortingDataTransfer _sortingDataTransfer;

    /// <summary>The scan estimate  computed when it chose IndexOrderStreaming.</summary>
    private double _rawStreamScanEstimate;

    public SortingMatch(IndexSearcher searcher, in TInner inner, OrderMetadata orderMetadata, in CancellationToken cancellationToken, NullsSortMode defaultNullsSortMode, int take = -1)
    {
        _searcher = searcher;
        _inner = inner;
        _orderMetadata = orderMetadata;
        _cancellationToken = cancellationToken;
        _nullFirst = (_orderMetadata.NullsSortMode ?? defaultNullsSortMode) == NullsSortMode.NullsSmallest;
        _take = take;
        _alreadyReadIdx = 0;
        _results = new ContextBoundNativeList<long>(searcher.Allocator);
        TotalResults = NotStarted;

        if (_orderMetadata.HasBoost)
        {
            // Score-sorted: the score pass re-reads every leaf's bitmap AFTER the plan folds them into the
            // result accumulator. Tell the inner CompiledQueryMatch to clone leaves into the fold so their
            // BitmapState survives intact for scoring. Must be set before the fold runs (first Count/Fill below).
            CompiledQueryMatch.MarkPreserveLeavesForScoring(_inner);
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

        public void Init<T>(T parent) => throw new NotSupportedException();

        public void Reset() => throw new NotSupportedException();

        public int Fill(Span<long> results, long lastId, bool includeMax) => throw new NotSupportedException();
        
        public bool Skip(long count) => throw new NotSupportedException();

        public bool MoveNext(out long value) => throw new NotSupportedException();

        public bool MoveNext<TLookupKey>(out TLookupKey key, out long value, out bool hasPreviousValue) => throw new NotSupportedException();

        public void Seek<TLookupKey>(TLookupKey key) => throw new NotSupportedException();
    }
    
    private struct NoIterationOptimization : ILookupIterator
    {
        public bool IsForward => throw new NotSupportedException($"{nameof(NoIterationOptimization)} has no direction and should not be used in parts of code where it is required.");

        
        public void Init<T>(T parent) => throw new NotSupportedException();

        public void Reset() => throw new NotSupportedException();

        public int Fill(Span<long> results, long lastId = long.MaxValue, bool includeMax = true) => throw new NotSupportedException();
        
        public bool Skip(long count) => throw new NotSupportedException();

        public bool MoveNext(out long value) => throw new NotSupportedException();

        public bool MoveNext<TLookupKey>(out TLookupKey key, out long value, out bool hasPreviousValue) => throw new NotSupportedException();

        public void Seek<TLookupKey>(TLookupKey key) => throw new NotSupportedException();
    }
        
    private static delegate*<SortingMatch<TInner>, Span<long>, int> SortBy<TEntryComparer,TFwdIt,TBackIt>(OrderMetadata metadata)
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


    private static int Fill<TEntryComparer, TDirection>(SortingMatch<TInner> match, Span<long> matches)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
        where TDirection : struct, ILookupIterator
    {
        // This method should also be re-entrant for the case where we have already pre-sorted everything, and
        // we will just need to acquire via pages the totality of the results.
        if (match.TotalResults == NotStarted)
        {
            if (match._inner is IBitmapQueryMatch bitmapMatch)
            {
                // First Count call will initialize the bitmap, putting that *outside* the sorting time bookkeeping intentionally
                match.TotalResults = bitmapMatch.Count;
                if (match.TotalResults == 0)
                    return 0;

                long sortStart = Stopwatch.GetTimestamp();

                if (typeof(TDirection) == typeof(RandomDirection))
                {
                    match.SortStrategy = CoraxSortingStrategy.RandomOrder;
                    SampleRandomOrder(match, bitmapMatch);
                }
                else if (typeof(TDirection) == typeof(NoIterationOptimization) || match._orderMetadata.MayHaveMissingEntries)
                {
                    // Score/spatial/alphanumeric: no index to walk, must materialize + heap sort.
                    // IndexOrderStreaming only walks tree terms + null/nonExisting posting lists, so missing entries requires InMemorySort
                    match.SortStrategy = CoraxSortingStrategy.InMemorySort;
                    match.GateDecision = SortStrategyDecision.NotIterableSortField;
                    SortInMemory<TEntryComparer>(match, bitmapMatch);
                }
                else if (ShouldUseIndexOrderStreaming(match, bitmapMatch))
                {
                    // Cost gate chose streaming; an InMemorySort pin overrides it (forces the bounded sort).
                    if (match.ForcedStrategy == CoraxSortingStrategy.InMemorySort)
                    {
                        match.SortStrategy = CoraxSortingStrategy.InMemorySort;
                        SortInMemory<TEntryComparer>(match, bitmapMatch);
                    }
                    else
                    {
                        match.SortStrategy = CoraxSortingStrategy.IndexOrderStreaming;
                        StreamInIndexOrder<TEntryComparer, TDirection>(match, bitmapMatch);
                    }
                }
                else if (match.ForcedStrategy == CoraxSortingStrategy.IndexOrderStreaming)
                {
                    // Cost gate rejected streaming, but the query explicitly pinned it
                    match.SortStrategy = CoraxSortingStrategy.IndexOrderStreaming;
                    StreamInIndexOrder<TEntryComparer, TDirection>(match, bitmapMatch);
                }
                else
                {
                    // Cost model rejected the streaming scan: Materialize the candidates and sort.
                    match.SortStrategy = CoraxSortingStrategy.InMemorySort;
                    SortInMemory<TEntryComparer>(match, bitmapMatch);
                }

                match.SortingTimeInTicks += Stopwatch.GetTimestamp() - sortStart;
            }
            else
            {
                // Non-bitmap path (VectorSearchMatch, PostFilterMatch, scoring matches, etc.), must fully drain
                SortComputedResults<TEntryComparer>(match);
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

    /// <summary>
    /// Chooses between IndexOrderStreaming (sequential index walk halted by a limit) 
    /// and InMemorySort (materializing candidates and heap-sorting).
    /// 
    /// Compares the estimated scan steps against candidates scaled by <see cref="IndexStreamingVsInMemorySortCostRatio"/> 
    /// to account for streaming being significantly cheaper per entry than random-lookup sorting.
    /// Heavy scans or missing limits automatically fall back to InMemorySort.
    ///
    /// One streamed entry is a sequential FastPFor posting decode plus an O(1) candidate-bitmap test, while one
    /// sorted candidate pays a random entries→terms lookup (GetFor), a random term-blob fetch (GetAll), and its
    /// share of an O(N*logN) comparison sort.
    ///
    /// A selective WHERE with a large/absent LIMIT degenerates into a near-full index walk — the no-LIMIT guard steers
    /// that to InMemorySort.
    /// </summary>
    private static bool ShouldUseIndexOrderStreaming(SortingMatch<TInner> match, IBitmapQueryMatch bitmapMatch)
    {
        long candidates = match.TotalResults; // == bitmapMatch.Count, already set by the caller
        long indexSize = match._searcher.NumberOfEntries;

        // No LIMIT (or a limit that can't cut below the candidate count): streaming will scan it all, in memory just the query matches
        if (match._take < 0 || match._take >= candidates)
        {
            match.GateDecision = SortStrategyDecision.NoLimitFullScan;
            return false;
        }

        // Expected entries scanned to collect `take` matches, assuming candidates are spread uniformly across the index
        double estimatedScan = (double)match._take * indexSize / candidates; // double to avoid overflow
        match._rawStreamScanEstimate = estimatedScan; // retained for the EWMA update on completion/bailout
        match.StreamScanEstimateRaw = estimatedScan;

        double inflationFactor = 1;
        if (bitmapMatch is CompiledQueryMatch { CompiledPlan: { } cp })
        {
            // Correct the uniform-distribution estimate by what this plan has actually scanned in the past.
            // Self-correcting by inflating / deflating estimation with prior queries' results 
            var inflation = cp.GetOrCreateStreamScanInflation().Factor;
            if (inflation > 0)
            {
                inflationFactor = inflation;
                estimatedScan *= inflation;
            }
        }

        match.StreamScanInflationFactor = inflationFactor;
        match.StreamScanEstimateInflated = estimatedScan;

        // Cost-weighted: a streamed entry is far cheaper than a sorted candidate (see the ratio's doc).
        double threshold = candidates * IndexStreamingVsInMemorySortCostRatio;
        match.GateThreshold = threshold;

        bool stream = estimatedScan < threshold;
        match.GateDecision = stream ? SortStrategyDecision.StreamCheaper : SortStrategyDecision.SortCheaper;
        return stream;
    }

    private static void SampleRandomOrder(SortingMatch<TInner> match, IBitmapQueryMatch bitmapMatch)
    {
        var random = new Random(match._orderMetadata.RandomSeed);
        int take = match._take;
        ref var bitmap = ref bitmapMatch.BitmapState;
        long totalCount = bitmapMatch.Count;

        if (totalCount == 0)
            return;

        if (take < 0)
        {
            // No LIMIT: materialize the whole bitmap in one Fill, then shuffle.
            match._results.EnsureCapacityFor((int)totalCount);
            Span<long> bulk = match._results.ToFullCapacitySpan();
            int filled = bitmapMatch.Fill(bulk);
            match._results.Count = filled;

            random.Shuffle(match._results.ToSpan());
        }
        else
        {
            // With LIMIT k: pick k random ranks from [0, totalCount), deduplicated,
            // then resolve all ranks to entry IDs in a single bulk Select call —
            // one container walk instead of one per rank.
            var allocator = match._searcher.Allocator;
            int k = (int)Math.Min(take, totalCount);
            match._results.EnsureCapacityFor(k);

            // Generate k unique random ranks using Floyd's algorithm (O(k), no rejection).
            var selected = new HashSet<long>(k);
            for (long i = totalCount - k; i < totalCount; i++)
            {
                long r = random.NextInt64(i + 1);
                if (selected.Add(r) == false)
                    selected.Add(i);
            }

            // Materialize ranks into a contiguous buffer; results land directly in _results.
            using var ranksList = new ContextBoundNativeList<long>(allocator, k);
            foreach (long rank in selected)
                ranksList.AddUnsafe(rank);

            // Floyd's only emits ranks in [0, totalCount), so every result is valid.
            match._results.Count = k;
            bitmap.Select(allocator, ranksList.ToSpan(), match._results.ToSpan());
        }
    }
    
    internal ref struct SortedIndexReader<TDirection> : IDisposable
        where TDirection : struct, ILookupIterator
    {
        private PostingList.Iterator _postListIt;
        private FastPForBufferedReader _smallListReader;
        private TDirection _termsIt;
        private readonly long _min;
        private readonly long _max;
        private readonly long _nonExistingPostingListId;
        private readonly long _nullPostingListId;

        private readonly bool _nullFirst;
        private readonly bool _isForward;
        private readonly IndexSearcher _searcher;
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
        private bool _nonExistingPostingListRead;
        private bool _nullPostingListRead;

        /// <summary>The iterator <paramref name="it"/> is assumed to be already positioned by the caller
        /// (caller is responsible for Reset + optional Seek).</summary>
        public SortedIndexReader(LowLevelTransaction llt, IndexSearcher searcher, TDirection it, FieldMetadata metadata, long min, long max, bool nullFirst, bool isForward)
        {
            _termsIt = it;
            _min = min;
            _max = max;
            _nullFirst = nullFirst;
            _isForward = isForward;
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

            _nonExistingPostingListRead = searcher.TryGetPostingListForNonExisting(metadata, out _nonExistingPostingListId) == false;
            _nullPostingListRead = searcher.TryGetPostingListForNull(metadata, out _nullPostingListId) == false;
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
                            long entryId = (long)EntryIdEncodings.GetContainerId(postingListId);
                            if(entryId >= _min && entryId <= _max)
                                sortedIds[currentIdx++] = entryId;
                            break;
                        case TermIdMask.SmallPostingList:
                            var item = _containerItems[_smallPostingListIndex++];
                            _ = VariableSizeEncoding.Read<int>(item.Address, out var offset); // discard count here
                            var start = FastPForDecoder.ReadStart(item.Address + offset);
                            if((long)EntryIdEncodings.DecodeAndDiscardFrequency(start) > _max)
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
            
            bool nullsFirst = _isForward ? _nullFirst : !_nullFirst;
            var buffer = new Span<long>(_itBuffer, BufferSize);
            if (nullsFirst)
                LoadNonExistingAndNullIntoBuffer(buffer);
            
            
            _bufferCount += _termsIt.Fill(buffer.Slice(_bufferCount));
            if (_bufferCount == 0)
            {
                if (nullsFirst || (_nonExistingPostingListRead && _nullPostingListRead))
                    return;
                
                LoadNonExistingAndNullIntoBuffer(buffer);
            }
            
            for (int i = 0; i < _bufferCount; i++)
            {
                var termType = (TermIdMask)_itBuffer[i] & TermIdMask.EnsureIsSingleMask;
                if (termType == TermIdMask.SmallPostingList)
                {
                    var smallSetId = EntryIdEncodings.GetContainerId(_itBuffer[i]);
                    _smallPostListIds.Add((long)smallSetId);
                }
            }

            _smallPostingListIndex = 0;
            if (_smallPostListIds.Count == 0)
                return;

            Container.GetAll(_llt, _smallPostListIds.ToSpan(), new Span<UnmanagedSpan>(_containerItems, _smallPostListIds.Count), _pageLocator);

            
        }
        
        void LoadNonExistingAndNullIntoBuffer(Span<long> buffer)
        {
            // nullFirst:  non-existing < null < normal values
            // nullLast:   normal values < null < non-existing
            bool nullsFirst = _isForward ? _nullFirst : !_nullFirst;
            if (nullsFirst)
            {
                LoadNonExistingIntoBuffer(buffer);
                LoadNullIntoBuffer(buffer);
            }
            else
            {
                LoadNullIntoBuffer(buffer);
                LoadNonExistingIntoBuffer(buffer);
            }
        }

        void LoadNonExistingIntoBuffer(Span<long> buffer)
        {
            if (_nonExistingPostingListRead == false)
            {
                buffer[_bufferCount] = _nonExistingPostingListId;
                _nonExistingPostingListRead = true;
                _bufferCount += 1;
            }
        }

        void LoadNullIntoBuffer(Span<long> buffer)
        {
            if (_nullPostingListRead == false)
            {
                buffer[_bufferCount] = _nullPostingListId;
                _nullPostingListRead = true;
                _bufferCount += 1;
            }
        }

        private void ReadLargePostingList(Span<long> sortedIds, ref int currentIdx)
        {
            if (_postListIt.Fill(sortedIds[currentIdx..], out var read) == false || (long)EntryIdEncodings.DecodeAndDiscardFrequency(sortedIds[currentIdx + read - 1]) > _max)
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
            // _smallListReader is lazily created only when a SmallPostingList is encountered; a default
            // instance has a null allocator and would throw inside FastPForDecoder.Dispose (RavenDB-25281).
            if (_smallListReader.WasInitialized)
                _smallListReader.Dispose();
            _smallPostListIds.Dispose();
            _containerItemsScope.Dispose();
            _itBufferScope.Dispose();
        }
    }

    /// <summary>
    /// Walk the CompactTree index in sorted order, intersecting each batch of entry IDs
    /// with the bitmap via AndWith. Stops early once _take results are collected.
    /// Avoids full materialization by intersecting directly against the bitmap.
    /// </summary>
    private static void StreamInIndexOrder<TEntryComparer, TDirection>(
        SortingMatch<TInner> match, IBitmapQueryMatch bitmapMatch)
        where TDirection : struct, ILookupIterator
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;
        var entryCmp = default(TEntryComparer);

        int maxResults = match._take == -1 ? int.MaxValue : match._take;

        // Runtime escape hatch: the cost gate assumed uniform candidate spread; if we scan too many entries without hitting the
        // end of the query, we bail to in memory sort.  Erring late is cheap (extra sequential posting reads) vs erring early
        // (forfeiting streaming's win when it would have finished soon).
        long scanBailoutThreshold = match.TotalResults * IndexStreamingVsInMemorySortCostRatio;
        bool forceUsingOnlyIndex = match.ForcedStrategy == CoraxSortingStrategy.IndexOrderStreaming;

        // Per-plan learning: record (entries actually scanned / the gate's uniform estimate) so a future query for this plan can have a better estimate
        var scanInflation = forceUsingOnlyIndex is false && bitmapMatch is CompiledQueryMatch { CompiledPlan: { } cp } ? cp.GetOrCreateStreamScanInflation() : null;

        using var sortedIdsScope = allocator.Allocate(sizeof(long) * SortBatchSize, out ByteString bs);
        Span<long> sortedIdBuffer = new(bs.Ptr, SortBatchSize);

        using var emittedBitmap = new RoaringBitmap(allocator);

        // Seek optimization: when the WHERE field matches the ORDER BY field, skip walking tree terms that can't match by seeking the underlying iterator to the boundary value.
        object hintValue = null;
        if (bitmapMatch is CompiledQueryMatch { SortHint: { } hint } && SliceEqualsUtf8(entryCmp.GetSortFieldName(match), hint.FieldName))
        {
            hintValue = hint.Value;
        }

        using var reader = GetReader(bitmapMatch.MinEntryId, bitmapMatch.MaxEntryId, hintValue);

        while (match._results.Count < maxResults)
        {
            match._cancellationToken.ThrowIfCancellationRequested();

            if (forceUsingOnlyIndex == false && match.EntriesStreamed > scanBailoutThreshold)
            {
                // Degenerate walk: scanned too much for too few hits. Discard the streamed prefix and re-sort the full candidate set via SortInMemory.
                // EntriesStreamed is kept so the wasted scan stays visible in the query plan graph.
                match.SortStrategy = CoraxSortingStrategy.IndexOrderFallbackToInMemorySort;
                scanInflation?.Observe(match.EntriesStreamed, (long)match._rawStreamScanEstimate);
                match._results.Clear();
                SortInMemory<TEntryComparer>(match, bitmapMatch);
                return;
            }

            var read = reader.Read(sortedIdBuffer);
            if (read == 0)
                break;

            match.EntriesStreamed += read; // sort-index IDs read before intersection

            // Intersect this batch with the WHERE bitmap, then dedup against the emitted bitmap in a single pass
            read = bitmapMatch.BitmapState.AndWith(sortedIdBuffer, read); 
            read = emittedBitmap.DedupAddNew(sortedIdBuffer, read);

            int toAdd = Math.Min(read, maxResults - match._results.Count);
            match._results.AddRange(sortedIdBuffer[..toAdd]);
        }

        // Streaming completed within budget: feed the observed scan back so the gate keeps trusting this plan.
        scanInflation?.Observe(match.EntriesStreamed, (long)match._rawStreamScanEstimate);


        [SkipLocalsInit]
        SortedIndexReader<TDirection> GetReader(long min, long max, object h)
        {
            if (typeof(TDirection) == typeof(Lookup<CompactTree.CompactKeyLookup>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<CompactTree.CompactKeyLookup>.BackwardIterator))
            {
                var termsTree = match._searcher.GetTermsFor(entryCmp.GetSortFieldName(match));
                var it = termsTree.IterateValues<TDirection>();
                it.Reset();
                if (h is string strVal)
                {
                    var compactKey = llt.AcquireCompactKey();
                    int byteCount = System.Text.Encoding.UTF8.GetMaxByteCount(strVal.Length);
                    Span<byte> stackBuf = byteCount switch
                    {
                        <= Utf8StackAllocThreshold => stackalloc byte[Utf8StackAllocThreshold],
                        _ => GrowUtf8Buffer(byteCount)
                    };
                    int written = System.Text.Encoding.UTF8.GetBytes(strVal, stackBuf);
                    compactKey.Set(stackBuf[..written]);
                    compactKey.ChangeDictionary(termsTree.DictionaryId);
                    it.Seek(new CompactTree.CompactKeyLookup(compactKey));
                }
                return new SortedIndexReader<TDirection>(llt, match._searcher, it, match._orderMetadata.Field, min, max, match._nullFirst, match._orderMetadata.Ascending);
            }

            if (typeof(TDirection) == typeof(Lookup<Int64LookupKey>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<Int64LookupKey>.BackwardIterator))
            {
                var termsTree = match._searcher.GetLongTermsFor(entryCmp.GetSortFieldName(match));
                var it = termsTree.Iterate<TDirection>();
                it.Reset();
                if (h is long longVal)
                    it.Seek(new Int64LookupKey(longVal));
                return new SortedIndexReader<TDirection>(llt, match._searcher, it, match._orderMetadata.Field, min, max, match._nullFirst, match._orderMetadata.Ascending);
            }

            if (typeof(TDirection) == typeof(Lookup<DoubleLookupKey>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<DoubleLookupKey>.BackwardIterator))
            {
                var termsTree = match._searcher.GetDoubleTermsFor(entryCmp.GetSortFieldName(match));
                var it = termsTree.Iterate<TDirection>();
                it.Reset();
                if (h is double doubleVal)
                    it.Seek(new DoubleLookupKey(doubleVal));
                return new SortedIndexReader<TDirection>(llt, match._searcher, it, match._orderMetadata.Field, min, max, match._nullFirst, match._orderMetadata.Ascending);
            }

            throw new NotSupportedException(typeof(TDirection).FullName);
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static byte[] GrowUtf8Buffer(int byteCount)
    {
        ref byte[] buf = ref Utf8ThreadBuffer;
        if (buf == null || buf.Length < byteCount)
            buf = new byte[Bits.PowerOf2(byteCount)];
        return buf;
    }

    [SkipLocalsInit]
    private static bool SliceEqualsUtf8(Slice slice, string s)
    {
        var sliceSpan = slice.AsReadOnlySpan();
        int byteCount = System.Text.Encoding.UTF8.GetMaxByteCount(s.Length);
        Span<byte> span = byteCount switch
        {
            <= Utf8StackAllocThreshold => stackalloc byte[Utf8StackAllocThreshold],
            _ => GrowUtf8Buffer(byteCount)
        };
        int written = System.Text.Encoding.UTF8.GetBytes(s, span);
        return written == sliceSpan.Length && sliceSpan.SequenceEqual(span[..written]);
    }

    /// <summary>
    /// For sort types without an index to walk (score, spatial, alphanumeric, random), materialize all bitmap entries directly and heap sort.
    /// </summary>
    private static void SortInMemory<TEntryComparer>(SortingMatch<TInner> match, IBitmapQueryMatch bitmapMatch)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
        var allocator = match._searcher.Allocator;

        if (match.TotalResults > int.MaxValue)
            throw new InvalidOperationException($"TotalResults ({match.TotalResults}) exceeds int.MaxValue — cannot materialize all bitmap entries for sorting.");

        int total = (int)match.TotalResults;

        // TotalResults == bitmapMatch.Count, so one Fill call covers everything.
        using var scope = allocator.Allocate(total, out Span<long> allMatches);
        int filled = bitmapMatch.Fill(allMatches);

        if (filled == 0)
            return;

        // The bitmap iterator yields entry ids ascending, so the score comparer can take the sorted fast path.
        match.CandidatesAreSorted = true;
        SortResults<TEntryComparer>(match, allMatches[..filled]);
    }
    
    /// <summary>Drain all results from the inner match via Fill, then heap sort.</summary>
    private static void SortComputedResults<TEntryComparer>(SortingMatch<TInner> match)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
        // Draining the inner match (Count + the Fill loop) is the inner query's execution, not sort work,
        // so it is deliberately left untimed here — only the SortResults call below is charged to the sort.
        using var scope = SortingHelpers.DrainMatch(ref match._inner, match._searcher.Allocator, out var allMatches);

        match.TotalResults = allMatches.Length;
        if (match.TotalResults == 0)
            return;

        match.CandidatesAreSorted = true;

        long sortStart = Stopwatch.GetTimestamp();
        SortResults<TEntryComparer>(match, allMatches);
        match.SortingTimeInTicks += Stopwatch.GetTimestamp() - sortStart;
    }

    private static void SortResults<TEntryComparer>(SortingMatch<TInner> match, Span<long> batchResults)
        where TEntryComparer : struct,  IEntryComparer, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;

        var sizeToAllocate = batchResults.Length * (sizeof(long) + sizeof(UnmanagedSpan));

        //OrderBySpatial relies on this order of data. If you change it, please review the spatial ordering to ensure that everything works fine: [[ids], [terms], [spatial_distances]].
        if (match._sortingDataTransfer.IncludeDistances)
            sizeToAllocate += batchResults.Length * sizeof(SpatialResult);
        
        using var bufScope = allocator.Allocate(sizeToAllocate, out ByteString bs);
        Span<long> batchTermIds = new(bs.Ptr, batchResults.Length);
        UnmanagedSpan* termsPtr = (UnmanagedSpan*)(bs.Ptr + batchResults.Length * sizeof(long));

        TEntryComparer entryComparer = new();
        entryComparer.Init(match);
        
        entryComparer.SortBatch(match, llt, llt.PageLocator, batchResults, batchTermIds, termsPtr);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer)
    {
        _sortingDataTransfer = sortingDataTransfer;
        if (_sortingDataTransfer.IncludeScores)
            _scoresResults = new(_searcher.Allocator);
        if (_sortingDataTransfer.IncludeDistances)
            _distancesResults = new(_searcher.Allocator);
    }

    public override long Count => _inner.Count;

    public override bool IsBoosting => _inner.IsBoosting || _orderMetadata.FieldType == MatchCompareFieldType.Score;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int Fill(Span<long> matches) => _fillFunc(this, matches);


    public override QueryInspectionNode Inspect()
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

        if (SortStrategy is { } strategy)
            parameters["Strategy"] = strategy.ToString();

        if (GateDecision != SortStrategyDecision.NotEvaluated)
        {
            parameters["StrategyReason"] = GateDecision.ToString();
            if (GateDecision is SortStrategyDecision.StreamCheaper or SortStrategyDecision.SortCheaper)
            {
                parameters["StreamScanEstimate"] = StreamScanEstimateInflated.ToString("N0", CultureInfo.InvariantCulture);
                if (Math.Abs(StreamScanInflationFactor - 1) > 0.0001)
                {
                    parameters["StreamScanRaw"] = StreamScanEstimateRaw.ToString("N0", CultureInfo.InvariantCulture);
                    parameters["StreamScanInflation"] = StreamScanInflationFactor.ToString("F2", CultureInfo.InvariantCulture);
                }
                parameters["StreamGateThreshold"] = GateThreshold.ToString("N0", CultureInfo.InvariantCulture);
            }
        }

        if (SortingTimeInTicks > 0)
            parameters["Ms"] = (SortingTimeInTicks / (Stopwatch.Frequency / 1000.0)).ToString("F3", CultureInfo.InvariantCulture);

        if (TotalResults >= 0)
        {
            parameters["Incoming"] = TotalResults.ToString("N0");
            long output = _take >= 0 ? Math.Min(_take, TotalResults) : TotalResults;
            parameters["Output"] = output.ToString("N0");
        }

        if (EntriesStreamed > 0)
            parameters["EntriesStreamed"] = EntriesStreamed.ToString();

        return new QueryInspectionNode($"{nameof(SortingMatch)}",
            children: [_inner.Inspect()],
            parameters: parameters);
    }

    public override void Dispose()
    {
        _results.Dispose();
        _entriesBufferScope.Dispose();
        _scoresResults.Dispose();
        _distancesResults.Dispose();
        (_inner as IDisposable)?.Dispose();
    }

    string DebugView => Inspect().ToString();
}
