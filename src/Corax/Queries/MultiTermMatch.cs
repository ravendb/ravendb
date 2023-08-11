using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Util.PFor;

namespace Corax.Queries
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct MultiTermMatch<TTermProvider> : IQueryMatch
        where TTermProvider : ITermProvider
    {
        private const int InitialFrequencyHolders = 64;

        private readonly bool _isBoosting;
        private long _totalResults;
        private long _current;
        private long _currentIdx;
        private bool _doNotSortResults;
        private QueryCountConfidence _confidence;

        private Bm25Relevance[] _frequenciesHolder;
        private int _currentFreqIdx;

        //In case of streaming we cannot sort the results since the order will not be persisted. This is possible only in case when MTM is not in Binary AST and single document has only 1 term.
        private bool _doNotSortResultsDueToStreaming;

        private int FrequenciesHolderSize => _frequenciesHolder?.Length ?? 0;


        private TTermProvider _inner;
        private TermMatch _currentTerm;
        private MultiTermReader _termReader;
        private readonly ByteStringContext _context;

        public bool IsBoosting => _isBoosting;
        public long Count => _totalResults;
        public long Current => _currentIdx <= QueryMatch.Start ? _currentIdx : _current;

        public bool RequiresSortAfterFill;

        public bool DoNotSortResults()
        {
            _doNotSortResults = true;
            return true;
        }

        public QueryCountConfidence Confidence => _confidence;

        public MultiTermMatch(IndexSearcher indexSearcher, in FieldMetadata field, ByteStringContext context, TTermProvider inner, bool streamingEnabled,
            long totalResults = 0, QueryCountConfidence confidence = QueryCountConfidence.Low)
        {
            _inner = inner;
            _isBoosting = field.HasBoost;

            if (_inner.IsFillSupported && _isBoosting == false)
                _termReader = new(indexSearcher);
            else
            {
                var result = _inner.Next(out _currentTerm);
                if (result == false)
                    _current = QueryMatch.Invalid;
            }
            
            _context = context;
            _current = QueryMatch.Start;
            _currentIdx = QueryMatch.Start;
            _totalResults = totalResults;
            _confidence = confidence;


            _doNotSortResultsDueToStreaming = streamingEnabled;

            if (_isBoosting)
            {
                var pool = Bm25Relevance.RelevancePool ??= ArrayPool<Bm25Relevance>.Create();
                _frequenciesHolder = pool.Rent(InitialFrequencyHolders);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> buffer)
        {
            return _inner.IsFillSupported && _isBoosting == false
                ? FillWithReader(buffer)
                : FillWithTermMatches(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int FillWithReader(Span<long> buffer)
        {
            return _termReader.Read(ref this, buffer);
        }

        private unsafe struct MultiTermReader
        {
            private PostingList.Iterator _postListIt;
            private FastPForBufferedReader _smallListReader;
            private readonly long _max;
            private readonly IndexSearcher _searcher;
            private readonly ByteStringContext _allocator;

            private const int BufferSize = 1024;
            private readonly long* _itBuffer;
            private readonly UnmanagedSpan* _containerItems;
            private int _bufferIdx;
            private int _bufferCount;
            private int _smallPostingListIndex;
            private NativeIntegersList _smallPostListIds;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _itBufferScope, _containerItemsScope;
            private readonly PageLocator _pageLocator;

            public MultiTermReader(IndexSearcher searcher, long max = long.MaxValue)
            {
                _searcher = searcher;
                _max = max;
                _allocator = searcher.Allocator;
                _postListIt = default;
                _smallListReader = default;
                _smallPostListIds = new NativeIntegersList(_allocator, BufferSize);
                _bufferCount = _bufferIdx = 0;
                _itBufferScope = _allocator.Allocate(BufferSize * sizeof(long), out ByteString bs);
                _itBuffer = (long*)bs.Ptr;
                _containerItemsScope = _allocator.Allocate(BufferSize * sizeof(UnmanagedSpan), out bs);
                _containerItems = (UnmanagedSpan*)bs.Ptr;
                _pageLocator = new PageLocator(searcher._transaction.LowLevelTransaction, BufferSize);
            }

            public void Reset(ref MultiTermMatch<TTermProvider> match)
            {
                match._inner.Reset();
                _smallPostListIds.Clear();
                _postListIt = default;
                _smallListReader = default;
            }

            public int Read(ref MultiTermMatch<TTermProvider> match, Span<long> sortedIds)
            {
                int postingListCalls = 0;
                fixed (long* pSortedIds = sortedIds)
                {
                    int currentIdx = 0;
                    // here we resume the *previous* operation
                    if (_smallListReader.IsValid)
                    {
                        postingListCalls++;
                        ReadSmallPostingList(pSortedIds, sortedIds.Length, ref currentIdx);
                    }
                    else if (_postListIt.IsValid)
                    {
                        postingListCalls++;
                        ReadLargePostingList(sortedIds, ref currentIdx);
                    }

                    while (currentIdx < sortedIds.Length)
                    {
                        if (_bufferIdx == _bufferCount)
                        {
                            RefillBuffers(ref match);
                            if (_bufferCount == 0)
                                break;
                        }

                        var postingListId = _itBuffer[_bufferIdx++];
                        var termType = (TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask;
                        switch (termType)
                        {
                            case TermIdMask.Single:
                                long entryId = EntryIdEncodings.GetContainerId(postingListId);
                                if (entryId <= _max)
                                    sortedIds[currentIdx++] = entryId;
                                postingListCalls++;
                                break;
                            case TermIdMask.SmallPostingList:
                                var item = _containerItems[_smallPostingListIndex++];
                                _ = VariableSizeEncoding.Read<int>(item.Address, out var offset); // discard count here
                                var start = FastPForDecoder.ReadStart(item.Address + offset);
                                if (start > _max)
                                    continue;
                                _smallListReader = new FastPForBufferedReader(_allocator, item.Address + offset, item.Length - offset);
                                postingListCalls++;
                                ReadSmallPostingList(pSortedIds, sortedIds.Length, ref currentIdx);
                                break;
                            case TermIdMask.PostingList:
                                var postingList = _searcher.GetPostingList(postingListId);
                                _postListIt = postingList.Iterate();
                                postingListCalls++;
                                ReadLargePostingList(sortedIds, ref currentIdx);
                                break;
                            default:
                                throw new OutOfMemoryException(termType.ToString());
                        }
                    }

                    if (match is {_doNotSortResultsDueToStreaming: false, _doNotSortResults: false} && postingListCalls > 1)
                        currentIdx = Sorting.SortAndRemoveDuplicates(sortedIds[0..currentIdx]);

                    return currentIdx;
                }
            }

            private void RefillBuffers(ref MultiTermMatch<TTermProvider> provider)
            {
                _smallPostListIds.Clear();
                _bufferIdx = 0;
                _bufferCount = 0;
                _bufferCount = provider._inner.Fill(new Span<long>(_itBuffer, BufferSize));
                
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
                Container.GetAll(_searcher._transaction.LowLevelTransaction, _smallPostListIds.Items, _containerItems, long.MinValue, _pageLocator);
            }

            private void ReadLargePostingList(Span<long> sortedIds, ref int currentIdx)
            {
                if (_postListIt.Fill(sortedIds[currentIdx..], out var read) == false ||
                    sortedIds[read - 1] > _max)
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


#if !DEBUG
        [SkipLocalsInit]
#endif
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int FillWithTermMatches(Span<long> buffer)
        {
            if (_current == QueryMatch.Invalid)
                return 0;

            int count = 0;
            var bufferState = buffer;
            bool requiresSort = false;
            while (bufferState.Length > 0)
            {
                var read = _currentTerm.Fill(bufferState);

                if (read == 0)
                {
                    AddTermToBm25();
                    if (_inner.Next(out _currentTerm) == false)
                    {
                        _current = QueryMatch.Invalid;
                        goto End;
                    }

                    // We can prove that we need sorting and deduplication in the end. 
                    requiresSort |= count != buffer.Length;
                }

                count += read;
                bufferState = bufferState.Slice(read);
            }

            _current = count != 0 ? buffer[count - 1] : QueryMatch.Invalid;

            End:
            if (_doNotSortResultsDueToStreaming == false && _doNotSortResults == false && requiresSort && count > 1)
            {
                count = Sorting.SortAndRemoveDuplicates(buffer[0..count]);
            }

            return count;
        }

        private void UnlikelyGrowBufferOfTermMatches()
        {
            var pool = Bm25Relevance.RelevancePool;
            var handler = pool.Rent(2 * FrequenciesHolderSize);
            _frequenciesHolder.CopyTo(handler.AsSpan(0, _currentFreqIdx));

            pool.Return(_frequenciesHolder);
            _frequenciesHolder = handler;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            if (_inner.IsFillSupported && _isBoosting == false)
                return AndWithFill(buffer, matches);

            return AndWithTermMatch(buffer, matches);
        }

        private int AndWithFill(Span<long> buffer, int matches)
        {
            using var _ = _context.Allocate(3 * sizeof(long) * buffer.Length, out var bufferHolder);
            var longBuffer = MemoryMarshal.Cast<byte, long>(bufferHolder.ToSpan());
            _termReader.Reset(ref this);
            Span<long> results = longBuffer.Slice(0, buffer.Length);
            Span<long> incomingMatches = longBuffer.Slice(buffer.Length, buffer.Length);
            Span<long> localMatches = longBuffer.Slice(2 * buffer.Length, buffer.Length);

            var actualMatches = buffer.Slice(0, matches);
            actualMatches.CopyTo(incomingMatches);
            var currentMatchCount = 0;
            //ensure we're not out of range
            while (results.Length > 0 && Fill(localMatches.Slice(0, results.Length)) is var read and > 0)
            {
                var common = MergeHelper.And(results, localMatches.Slice(0, read), incomingMatches.Slice(0, matches));
                results = results.Slice(common);
                currentMatchCount += common;
            }

            longBuffer.Slice(0, currentMatchCount).CopyTo(buffer);
            return currentMatchCount;
        }

#if !DEBUG
        [SkipLocalsInit]
#endif
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int AndWithTermMatch(Span<long> buffer, int matches)
        {
            // We should consider the policy where it makes sense to actually implement something different to avoid
            // the N^2 check here. While could be interesting to do it now, we still dont know what are the conditions
            // that would trigger such optimization or if they are even necessary. The reason why is that buffer size
            // may offset some of the requirements for such a scan operation. Interesting approaches to consider include
            // evaluating directly, construct temporary data structures like bloom filters on subsequent iterations when
            // the statistics guarantee those approaches, etc. Currently we apply memoization but without any limit to 
            // size of the result and it's subsequent usage of memory. 

            using var _ = _context.Allocate(3 * sizeof(long) * buffer.Length, out var bufferHolder);
            var longBuffer = MemoryMarshal.Cast<byte, long>(bufferHolder.ToSpan());

            // PERF: We want to avoid to share cache lines, that's why the third array will move toward the end of the array. 
            Span<long> results = longBuffer.Slice(0, buffer.Length);
            Span<long> tmp = longBuffer.Slice(buffer.Length, buffer.Length);
            Span<long> tmp2 = longBuffer.Slice(2 * buffer.Length, buffer.Length);

            _inner.Reset();
            ResetBm25Buffer();

            var actualMatches = buffer.Slice(0, matches);

            bool hasData = _inner.Next(out _currentTerm);
            AddTermToBm25();

            long totalRead = _currentTerm.Count;

            int totalSize = 0;
            while (totalSize < buffer.Length && hasData)
            {
                actualMatches.CopyTo(tmp);
                var read = _currentTerm.AndWith(tmp, matches);
                if (read != 0)
                {
                    results[0..totalSize].CopyTo(tmp2);
                    totalSize = MergeHelper.Or(results, tmp2[0..totalSize], tmp[0..read]);
                    totalRead += _currentTerm.Count;
                }

                hasData = _inner.Next(out _currentTerm);
                AddTermToBm25();
            }

            // We will check if we can make a better decision next time. 
            if (!hasData)
            {
                _totalResults = totalRead;
                _confidence = QueryCountConfidence.High;
            }

            results[0..totalSize].CopyTo(buffer);

            return totalSize;
        }

        private void ResetBm25Buffer()
        {
            for (int idX = 0; idX < _currentFreqIdx; ++idX)
            {
                _frequenciesHolder[idX].Dispose();
                _frequenciesHolder[idX] = null;
            }

            _currentFreqIdx = 0;
        }

        private void AddTermToBm25()
        {
            if (_isBoosting && _currentTerm.Count != 0)
            {
                if (_currentFreqIdx >= FrequenciesHolderSize)
                    UnlikelyGrowBufferOfTermMatches();
                _frequenciesHolder[_currentFreqIdx] = _currentTerm._bm25Relevance;
                _currentFreqIdx += 1;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            if (_isBoosting == false)
                return;

            //We've to gather all data from already seen TermMatches to get proper ranking :)
            for (int idX = 0; idX < _currentFreqIdx; ++idX)
            {
                ref var currentRelevance = ref _frequenciesHolder[idX];
                currentRelevance.Score(matches, scores, boostFactor);
                currentRelevance.Dispose();
                currentRelevance = null;
            }

            Bm25Relevance.RelevancePool.Return(_frequenciesHolder);
            _frequenciesHolder = null;
            _currentFreqIdx = 0;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode(nameof(MultiTermMatch<TTermProvider>),
                children: new List<QueryInspectionNode> {_inner.Inspect()},
                parameters: new Dictionary<string, string>() {{nameof(IsBoosting), IsBoosting.ToString()}, {nameof(Count), $"{Count} [{Confidence}]"}});
        }

        public string DebugView => Inspect().ToString();
    }
}
